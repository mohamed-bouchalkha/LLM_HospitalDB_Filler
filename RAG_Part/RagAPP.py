print("✅ Script started...")
import os
import sys
import re
import torch

# -----------------------------
# 1. CONFIGURATION
# -----------------------------
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

try:
    from dotenv import load_dotenv
    load_dotenv()
    import pandas as pd
    from langchain_huggingface import HuggingFaceEmbeddings
    from langchain_community.vectorstores import FAISS
    from langchain_groq import ChatGroq
    from langchain.prompts import PromptTemplate
    from langchain_core.documents import Document
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process
except ImportError as e:
    print(f"❌ Error: {e}")
    print("💡 Tip: Install fuzzywuzzy with: pip install fuzzywuzzy python-Levenshtein")
    sys.exit(1)

# -----------------------------
# DEVICE DETECTION
# -----------------------------
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {DEVICE}")
if DEVICE == "cuda":
    print(f"GPU detected: {torch.cuda.get_device_name(0)}")
else:
    print("No GPU detected. Using CPU.")

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def normalize_id(value):
    """Normalize any key to consistent string format"""
    try:
        if pd.isna(value):
            return "0"
        return str(int(float(value)))
    except (ValueError, TypeError):
        return str(value)

def expand_query(query):
    """Expand query with synonyms and related terms for better retrieval"""
    expansions = {
        'medication': ['medication', 'drug', 'prescription', 'medicine', 'pharmaceutical'],
        'observation': ['observation', 'test', 'measurement', 'result', 'reading', 'lab'],
        'procedure': ['procedure', 'surgery', 'operation', 'treatment', 'intervention'],
        'diagnosis': ['diagnosis', 'condition', 'disease', 'disorder', 'illness'],
        'event': ['event', 'encounter', 'visit', 'record', 'entry'],
        'doctor': ['doctor', 'physician', 'provider', 'clinician', 'dr'],
        'hospital': ['hospital', 'organization', 'facility', 'clinic', 'center'],
        'insurance': ['insurance', 'payer', 'coverage', 'insurer'],
    }
    
    query_lower = query.lower()
    expanded_terms = []
    
    for key, synonyms in expansions.items():
        if key in query_lower:
            expanded_terms.extend(synonyms)
    
    if expanded_terms:
        unique_terms = list(set(expanded_terms))
        return query + " " + " ".join(unique_terms)
    
    return query

def detect_query_actor(query):
    """Detect which actor and what information is being requested"""
    query_lower = query.lower()
    
    # Patient patterns (prioritize specific IDs)
    patient_patterns = [
        r'patient\s*(\d+)',
        r'patient\s+id\s*(\d+)',
        r'patient\s+number\s*(\d+)'
    ]
    
    # Provider patterns
    provider_patterns = [
        r'provider\s*(\d+)',
        r'provider\s+id\s*(\d+)',
        r'doctor\s*(\d+)',
        r'physician\s*(\d+)',
    ]
    
    # Organization patterns
    org_patterns = [
        r'organization\s*(\d+)',
        r'org\s*(\d+)',
        r'hospital\s*(\d+)',
        r'facility\s*(\d+)',
    ]
    
    # Payer patterns
    payer_patterns = [
        r'payer\s*(\d+)',
        r'insurance\s*(\d+)',
    ]
    
    # Check for specific IDs first
    for pattern in patient_patterns:
        match = re.search(pattern, query_lower)
        if match:
            return 'patient', normalize_id(match.group(1))
    
    for pattern in provider_patterns:
        match = re.search(pattern, query_lower)
        if match:
            return 'provider', normalize_id(match.group(1))
    
    for pattern in org_patterns:
        match = re.search(pattern, query_lower)
        if match:
            return 'organization', normalize_id(match.group(1))
    
    for pattern in payer_patterns:
        match = re.search(pattern, query_lower)
        if match:
            return 'payer', normalize_id(match.group(1))
    
    # Generic keywords without specific identifiers
    if any(word in query_lower for word in ['patient']):
        return 'patient', None
    if any(word in query_lower for word in ['doctor', 'physician', 'provider', 'dr.']):
        return 'provider', None
    if any(word in query_lower for word in ['hospital', 'organization', 'facility', 'clinic']):
        return 'organization', None
    if any(word in query_lower for word in ['insurance', 'payer', 'coverage']):
        return 'payer', None
    
    return None, None

def fuzzy_match_score(query, text, threshold=60):
    """Calculate fuzzy match score between query and text
    Returns score (0-100) where higher means better match"""
    # Token set ratio handles word order differences
    token_set_score = fuzz.token_set_ratio(query.lower(), text.lower())
    # Partial ratio finds best matching substring
    partial_score = fuzz.partial_ratio(query.lower(), text.lower())
    # Combined score weighted toward token_set for relevance
    combined_score = (token_set_score * 0.7) + (partial_score * 0.3)
    return combined_score if combined_score >= threshold else 0

# -----------------------------
# ULTIMATE HYBRID RETRIEVER
# -----------------------------
class UltimateHybridRetriever:
    """Advanced retriever combining all strategies from both versions"""
    
    def __init__(self, vectorstore, actor_type=None, actor_id=None):
        self.vectorstore = vectorstore
        self.actor_type = actor_type
        self.actor_id = actor_id
    
    def get_relevant_documents(self, query):
        """Retrieve documents using ALL available strategies"""
        all_docs = []
        seen_content = set()
        
        expanded = expand_query(query)
        
        # STRATEGY 1: Semantic search with expanded query
        try:
            if self.actor_type and self.actor_id:
                filter_key = f"{self.actor_type}_id"
                docs1 = self.vectorstore.similarity_search(
                    expanded, 
                    k=150,
                    filter={filter_key: str(self.actor_id)}
                )
            else:
                docs1 = self.vectorstore.similarity_search(expanded, k=100)
            
            for doc in docs1:
                content_hash = hash(doc.page_content)
                if content_hash not in seen_content:
                    seen_content.add(content_hash)
                    all_docs.append(doc)
        except Exception as e:
            print(f"⚠️ Strategy 1 failed: {e}")
        
        # STRATEGY 2: MMR for diversity
        try:
            if self.actor_type and self.actor_id:
                filter_key = f"{self.actor_type}_id"
                docs2 = self.vectorstore.max_marginal_relevance_search(
                    query,
                    k=100,
                    fetch_k=300,
                    filter={filter_key: str(self.actor_id)}
                )
            else:
                docs2 = self.vectorstore.max_marginal_relevance_search(
                    query,
                    k=50,
                    fetch_k=150
                )
            
            for doc in docs2:
                content_hash = hash(doc.page_content)
                if content_hash not in seen_content:
                    seen_content.add(content_hash)
                    all_docs.append(doc)
        except Exception as e:
            print(f"⚠️ Strategy 2 failed: {e}")
        
        # STRATEGY 3: Keyword-based filtering (CRITICAL for comprehensive results)
        if self.actor_type and self.actor_id:
            try:
                filter_key = f"{self.actor_type}_id"
                # Get ALL documents for this actor
                all_actor_docs = self.vectorstore.similarity_search(
                    "",
                    k=1000,  # Increased to capture all events
                    filter={filter_key: str(self.actor_id)}
                )
                
                # Extract keywords from query
                keywords = [w for w in query.lower().split() if len(w) > 2]
                
                for doc in all_actor_docs:
                    content_lower = doc.page_content.lower()
                    # Add if ANY keyword matches OR if it's an event document
                    if (any(kw in content_lower for kw in keywords) or 
                        doc.metadata.get("type") == "event"):
                        content_hash = hash(doc.page_content)
                        if content_hash not in seen_content:
                            seen_content.add(content_hash)
                            all_docs.append(doc)
            except Exception as e:
                print(f"⚠️ Strategy 3 failed: {e}")
        
        # STRATEGY 3.5: Fuzzy matching for better content matching
        if self.actor_type and self.actor_id and len(all_docs) < 50:
            try:
                filter_key = f"{self.actor_type}_id"
                fuzzy_docs = self.vectorstore.similarity_search(
                    "",
                    k=1000,
                    filter={filter_key: str(self.actor_id)}
                )
                
                # Score documents using fuzzy matching
                scored_docs = []
                for doc in fuzzy_docs:
                    score = fuzzy_match_score(query, doc.page_content, threshold=50)
                    if score > 0:
                        scored_docs.append((doc, score))
                
                # Sort by fuzzy score and add top matches
                scored_docs.sort(key=lambda x: x[1], reverse=True)
                for doc, score in scored_docs[:50]:
                    content_hash = hash(doc.page_content)
                    if content_hash not in seen_content:
                        seen_content.add(content_hash)
                        all_docs.append(doc)
                        
                if len(scored_docs) > 0:
                    print(f"🔍 Fuzzy matching added {len([d for d, _ in scored_docs[:50] if hash(d.page_content) not in seen_content])} documents")
            except Exception as e:
                print(f"⚠️ Fuzzy strategy failed: {e}")
        
        # STRATEGY 4: Profile documents (to provide context)
        if self.actor_type and self.actor_id:
            try:
                filter_key = f"{self.actor_type}_id"
                profile_docs = self.vectorstore.similarity_search(
                    "",
                    k=10,
                    filter={
                        filter_key: str(self.actor_id),
                        "type": f"{self.actor_type}_profile"
                    }
                )
                
                for doc in profile_docs:
                    content_hash = hash(doc.page_content)
                    if content_hash not in seen_content:
                        seen_content.add(content_hash)
                        all_docs.append(doc)
            except Exception as e:
                print(f"⚠️ Strategy 4 failed: {e}")
        
        # STRATEGY 5: Related actor search (for cross-references)
        if self.actor_type and self.actor_id:
            try:
                # Search by related fields
                related_field = f"related_{self.actor_type}"
                docs5 = self.vectorstore.similarity_search(
                    expanded,
                    k=50
                )
                
                for doc in docs5:
                    if related_field in doc.metadata:
                        content_hash = hash(doc.page_content)
                        if content_hash not in seen_content:
                            seen_content.add(content_hash)
                            all_docs.append(doc)
            except Exception as e:
                print(f"⚠️ Strategy 5 failed: {e}")
        
        print(f"📊 Retrieved {len(all_docs)} unique documents")
        
        # Sort: prioritize event documents, then by fuzzy relevance if available
        event_docs = [d for d in all_docs if d.metadata.get("type") == "event"]
        profile_docs = [d for d in all_docs if d.metadata.get("type") != "event"]
        
        # For non-actor queries, apply fuzzy ranking to all results
        if not self.actor_type or not self.actor_id:
            scored_all = [(doc, fuzzy_match_score(query, doc.page_content, threshold=0)) 
                         for doc in all_docs]
            scored_all.sort(key=lambda x: x[1], reverse=True)
            return [doc for doc, score in scored_all]
        
        return event_docs + profile_docs

# -----------------------------
# MAIN
# -----------------------------
def main():
    DB_FOLDER = "faiss_index_ultimate_v6"

    print("📄 Initializing Embeddings...")
    embedding_function = HuggingFaceEmbeddings(
        model_name="BAAI/bge-base-en-v1.5",
        model_kwargs={'device': DEVICE}
    )

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=100,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )

    # --------------------------------------
    # BUILD DATABASE
    # --------------------------------------
    def build_database():
        print("\n⚙️ Building FAISS index...")

        try:
            fact_df = pd.read_csv("data/fact_patient_events.csv")
            patient_df = pd.read_csv("data/dim_patient.csv")
            provider_df = pd.read_csv("data/dim_provider.csv")
            payer_df = pd.read_csv("data/dim_payer.csv")
            org_df = pd.read_csv("data/dim_organization.csv")
            date_df = pd.read_csv("data/dim_date.csv")
        except FileNotFoundError as e:
            print(f"❌ CSV files not found: {e}")
            sys.exit(1)

        print("\n📊 Data Diagnostics:")
        print(f"   Total patients: {len(patient_df)}")
        print(f"   Total providers: {len(provider_df)}")
        print(f"   Total organizations: {len(org_df)}")
        print(f"   Total payers: {len(payer_df)}")
        print(f"   Total events: {len(fact_df)}")

        # Renaming
        patient_df = patient_df.rename(columns={
            "city": "patient_city",
            "state": "patient_state",
            "zip": "patient_zip"
        })
        provider_df = provider_df.rename(columns={"name": "provider_name"})
        payer_df = payer_df.rename(columns={"name": "payer_name"})
        org_df = org_df.rename(columns={"name": "org_name", "city": "org_city", "state": "org_state"})

        print("   -> Merging tables...")
        df = (
            fact_df.merge(patient_df, on="patient_key", how="left")
                   .merge(provider_df, on="provider_key", how="left")
                   .merge(payer_df, on="payer_key", how="left")
                   .merge(org_df, on="org_key", how="left")
                   .merge(date_df, on="date_key", how="left")
        )

        print("   -> Creating enriched documents...")
        documents = []
        
        # Counters for each actor
        patient_event_count = {}
        provider_event_count = {}
        org_event_count = {}
        payer_event_count = {}

        # ========================================
        # 1. EVENT DOCUMENTS (Multiple representations)
        # ========================================
        print("   -> Creating event documents...")
        for _, row in df.iterrows():
            pid_str = normalize_id(row["patient_key"])
            provider_key = normalize_id(row["provider_key"]) if pd.notna(row["provider_key"]) else None
            org_key = normalize_id(row["org_key"]) if pd.notna(row["org_key"]) else None
            payer_key = normalize_id(row["payer_key"]) if pd.notna(row["payer_key"]) else None
            
            patient_event_count[pid_str] = patient_event_count.get(pid_str, 0) + 1
            if provider_key:
                provider_event_count[provider_key] = provider_event_count.get(provider_key, 0) + 1
            if org_key:
                org_event_count[org_key] = org_event_count.get(org_key, 0) + 1
            if payer_key:
                payer_event_count[payer_key] = payer_event_count.get(payer_key, 0) + 1
            
            category = str(row['event_category']) if pd.notna(row['event_category']) else "Event"
            
            # Create comprehensive metadata
            metadata = {
                "source": "warehouse",
                "patient_id": pid_str,
                "type": "event",
                "date": str(row['date_key']) if pd.notna(row['date_key']) else "",
                "category": category
            }
            
            if provider_key:
                metadata["provider_id"] = provider_key
                metadata["related_provider"] = str(row['provider_name'])
            if org_key:
                metadata["organization_id"] = org_key
                metadata["related_organization"] = str(row['org_name'])
            if payer_key:
                metadata["payer_id"] = payer_key
                metadata["related_payer"] = str(row['payer_name'])
            
            # REPRESENTATION 1: Detailed event (original version approach)
            text1 = f"{category} for Patient {pid_str}. "
            if pd.notna(row['description']):
                text1 += f"Description: {row['description']}. "
            if pd.notna(row['numeric_value']):
                text1 += f"Value: {row['numeric_value']} {row['units']}. "
            text1 += f"Date: {row['date_key']}. "
            text1 += f"Patient demographics: {row['gender']}, {row['patient_city']}. "
            
            if pd.notna(row['provider_name']):
                text1 += f"Provider: {row['provider_name']} ({row['specialty']}). "
            if pd.notna(row['org_name']):
                text1 += f"Organization: {row['org_name']}, {row['org_city']}, {row['org_state']}. "
            if pd.notna(row['payer_name']):
                text1 += f"Covered by: {row['payer_name']}. "
            
            documents.append(Document(page_content=text1, metadata=metadata.copy()))
            
            # REPRESENTATION 2: Simple summary for keyword matching
            text2 = f"Patient {pid_str} had {category.lower()} on {row['date_key']}."
            if pd.notna(row['description']):
                text2 += f" {row['description']}"
            
            documents.append(Document(page_content=text2, metadata=metadata.copy()))

        # ========================================
        # 2. PATIENT PROFILES (original approach)
        # ========================================
        print("   -> Creating patient profiles...")
        df['date_key_str'] = df['date_key'].astype(str)
        
        patient_event_summaries = df.groupby('patient_key').agg({
            'event_category': lambda x: ', '.join(x.dropna().astype(str).unique()[:10]),
            'date_key_str': lambda x: list(x.dropna().unique())
        }).reset_index()
        
        for _, row in patient_df.iterrows():
            pid_str = normalize_id(row["patient_key"])
            event_count = patient_event_count.get(pid_str, 0)

            content = f"Patient {pid_str}: {row['gender']}, born {row['birthdate']}, lives in {row['patient_city']}, {row['patient_state']} {row['patient_zip']}. "
            
            if event_count == 0:
                content += "No recorded medical events."
            else:
                content += f"Has {event_count} medical events. "
                
                patient_summary = patient_event_summaries[
                    patient_event_summaries['patient_key'].apply(lambda x: normalize_id(x) == pid_str)
                ]
                if not patient_summary.empty:
                    categories = patient_summary.iloc[0]['event_category']
                    if pd.notna(categories) and str(categories) != '':
                        content += f"Event types: {categories}. "

            documents.append(Document(
                page_content=content,
                metadata={
                    "source": "patient_dim",
                    "patient_id": pid_str,
                    "type": "patient_profile",
                    "event_count": str(event_count)
                }
            ))

        # ========================================
        # 3. PROVIDER PROFILES
        # ========================================
        print("   -> Creating provider profiles...")
        for _, row in provider_df.iterrows():
            provider_key = normalize_id(row["provider_key"])
            event_count = provider_event_count.get(provider_key, 0)
            
            provider_events = df[df['provider_key'].apply(lambda x: normalize_id(x) == provider_key)]
            unique_patients = provider_events['patient_key'].apply(normalize_id).unique()
            
            content = f"Provider {provider_key}: {row['provider_name']}, Specialty: {row['specialty']}. "
            content += f"Treated {len(unique_patients)} patients with {event_count} total events. "
            
            if len(unique_patients) > 0:
                patient_list = ', '.join(list(unique_patients)[:10])
                content += f"Patient IDs: {patient_list}."

            documents.append(Document(
                page_content=content,
                metadata={
                    "source": "provider_dim",
                    "provider_id": provider_key,
                    "type": "provider_profile",
                    "event_count": str(event_count),
                    "patient_count": str(len(unique_patients))
                }
            ))

        # ========================================
        # 4. ORGANIZATION PROFILES
        # ========================================
        print("   -> Creating organization profiles...")
        for _, row in org_df.iterrows():
            org_key = normalize_id(row["org_key"])
            event_count = org_event_count.get(org_key, 0)
            
            org_events = df[df['org_key'].apply(lambda x: normalize_id(x) == org_key)]
            unique_patients = org_events['patient_key'].apply(normalize_id).unique()
            unique_providers = org_events['provider_name'].dropna().unique()
            
            content = f"Organization {org_key}: {row['org_name']}, Location: {row['org_city']}, {row['org_state']}. "
            content += f"Served {len(unique_patients)} patients with {event_count} total events. "
            content += f"Has {len(unique_providers)} providers. "
            
            if len(unique_patients) > 0:
                patient_list = ', '.join(list(unique_patients)[:10])
                content += f"Patient IDs: {patient_list}."

            documents.append(Document(
                page_content=content,
                metadata={
                    "source": "org_dim",
                    "organization_id": org_key,
                    "type": "organization_profile",
                    "event_count": str(event_count),
                    "patient_count": str(len(unique_patients))
                }
            ))

        # ========================================
        # 5. PAYER PROFILES
        # ========================================
        print("   -> Creating payer profiles...")
        for _, row in payer_df.iterrows():
            payer_key = normalize_id(row["payer_key"])
            event_count = payer_event_count.get(payer_key, 0)
            
            payer_events = df[df['payer_key'].apply(lambda x: normalize_id(x) == payer_key)]
            unique_patients = payer_events['patient_key'].apply(normalize_id).unique()
            
            content = f"Payer {payer_key}: {row['payer_name']}. "
            content += f"Covers {len(unique_patients)} patients with {event_count} total events. "
            
            if len(unique_patients) > 0:
                patient_list = ', '.join(list(unique_patients)[:10])
                content += f"Patient IDs: {patient_list}."

            documents.append(Document(
                page_content=content,
                metadata={
                    "source": "payer_dim",
                    "payer_id": payer_key,
                    "type": "payer_profile",
                    "event_count": str(event_count),
                    "patient_count": str(len(unique_patients))
                }
            ))

        print(f"\n   -> Total documents: {len(documents)}")
        print(f"   -> Patients with events: {len(patient_event_count)}")
        print(f"   -> Providers with events: {len(provider_event_count)}")
        print(f"   -> Organizations with events: {len(org_event_count)}")
        print(f"   -> Payers with events: {len(payer_event_count)}")
        
        print("   -> Building FAISS index...")
        vectorstore = FAISS.from_documents(documents, embedding_function)
        vectorstore.save_local(DB_FOLDER)
        print("✅ Database built!\n")
        return vectorstore

    # Load or build database
    REBUILD_DB = False
    if REBUILD_DB or not os.path.exists(DB_FOLDER):
        vectorstore = build_database()
    else:
        print(f"✅ Loading FAISS index from {DB_FOLDER}...")
        try:
            vectorstore = FAISS.load_local(DB_FOLDER, embedding_function, allow_dangerous_deserialization=True)
        except:
            vectorstore = build_database()

    # LLM Setup
    print("\n🔌 Connecting to Groq...")
    llm = ChatGroq(temperature=0, model_name="llama-3.3-70b-versatile")

    template = """You are a Healthcare Data Warehouse Analyst with comprehensive access to patient records.
Answer using ONLY the provided context below.
If information is not in the context, say "I don't know".

IMPORTANT INSTRUCTIONS:
1. The context contains multiple document chunks - read ALL of them carefully
2. Event documents contain detailed information about specific medical events
3. Profile documents provide summary statistics and demographics
4. Provide (Docter) specific details from events when available, not just summaries

Context:
{context}

Question: {question}

Answer (be specific, detailed, and use information from the context):
"""

    prompt = PromptTemplate(template=template, input_variables=["context", "question"])

    # Interactive loop
    print("\n💬 System Ready. Type 'exit' or 'quit' to exit.\n")
    print("📝 Example Queries:")
    print("  🏥 PATIENT: 'What medications does patient 3 have?'")
    print("  👨‍⚕️ PROVIDER: 'Tell me about provider 1' or 'What patients did provider 5 treat?'")
    print("  🏢 ORGANIZATION: 'Tell me about organization 3' or 'What services at organization 1?'")
    print("  💳 PAYER: 'Tell me about payer 2' or 'Who is covered by payer 1?'")
    print("  📅 GENERAL: 'What procedures happened in 2020?' or 'Show me all diabetes cases'")
    print("  🔍 FUZZY: 'Find records about diabetis' or 'Show me surgry events' (handles typos!)\n")

    while True:
        try:
            query = input("\n🔍 Query: ")
            if query.lower() in ["exit", "quit"]:
                break
            if not query.strip():
                continue

            # Debug command
            if query.lower().startswith("debug"):
                parts = query.split()
                if len(parts) >= 3:
                    actor_type = parts[1]
                    actor_id = normalize_id(parts[2])
                    print(f"\n🔬 Debug {actor_type.upper()} {actor_id}:")
                    
                    try:
                        filter_key = f"{actor_type}_id"
                        debug_docs = vectorstore.similarity_search("", k=1000, filter={filter_key: actor_id})
                        print(f"Total documents: {len(debug_docs)}")
                        
                        event_docs = [d for d in debug_docs if d.metadata.get("type") == "event"]
                        profile_docs = [d for d in debug_docs if d.metadata.get("type") != "event"]
                        print(f"Event documents: {len(event_docs)}")
                        print(f"Profile documents: {len(profile_docs)}")
                        
                        for i, doc in enumerate(debug_docs[:5], 1):
                            print(f"\n--- Doc {i} ---")
                            print(f"Type: {doc.metadata.get('type')}")
                            print(f"Content: {doc.page_content[:200]}...")
                    except Exception as e:
                        print(f"Debug error: {e}")
                continue

            # Detect actor type and ID
            actor_type, actor_id = detect_query_actor(query)
            
            if actor_type:
                if actor_id:
                    print(f"🎯 Detected {actor_type.upper()} ID: {actor_id}")
                else:
                    print(f"🎯 Detected {actor_type.upper()} query (no specific ID)")

            # Use ultimate hybrid retriever
            retriever = UltimateHybridRetriever(vectorstore, actor_type, actor_id)
            retrieved_docs = retriever.get_relevant_documents(query)
            
            if len(retrieved_docs) == 0:
                print("💡 Result: I don't know - no relevant documents found.")
                continue
            
            # Build context (use more documents for comprehensive answers)
            context = "\n\n".join([doc.page_content for doc in retrieved_docs[:100]])
            
            # Get LLM response
            print("⏳ Thinking...", end="\r")
            messages = prompt.format(context=context, question=query)
            response = llm.invoke(messages)
            
            print(f"\n💡 Answer:\n{response.content}\n")
            print(f"📄 Retrieved {len(retrieved_docs)} documents ({len([d for d in retrieved_docs[:100] if d.metadata.get('type') == 'event'])} events)")

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()