"""
FastAPI Server for Healthcare Data Warehouse RAG System
Run with: uvicorn api:app --reload --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import os
import sys
import re
import torch
from contextlib import asynccontextmanager

# Configuration
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
    from fuzzywuzzy import fuzz
except ImportError as e:
    print(f"❌ Error: {e}")
    print("💡 Install: pip install fastapi uvicorn python-dotenv langchain-huggingface langchain-community langchain-groq fuzzywuzzy python-Levenshtein faiss-cpu torch pandas")
    sys.exit(1)

# Global variables
vectorstore = None
llm = None
embedding_function = None

# ========================================
# HELPER FUNCTIONS (from original script)
# ========================================

def normalize_id(value):
    """Normalize any key to consistent string format"""
    try:
        if pd.isna(value):
            return "0"
        return str(int(float(value)))
    except (ValueError, TypeError):
        return str(value)

def expand_query(query):
    """Expand query with synonyms and related terms"""
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
    
    patterns = {
        'patient': [r'patient\s*(\d+)', r'patient\s+id\s*(\d+)'],
        'provider': [r'provider\s*(\d+)', r'doctor\s*(\d+)', r'physician\s*(\d+)'],
        'organization': [r'organization\s*(\d+)', r'org\s*(\d+)', r'hospital\s*(\d+)'],
        'payer': [r'payer\s*(\d+)', r'insurance\s*(\d+)']
    }
    
    for actor_type, pattern_list in patterns.items():
        for pattern in pattern_list:
            match = re.search(pattern, query_lower)
            if match:
                return actor_type, normalize_id(match.group(1))
    
    # Generic keywords
    if 'patient' in query_lower:
        return 'patient', None
    if any(word in query_lower for word in ['doctor', 'physician', 'provider']):
        return 'provider', None
    if any(word in query_lower for word in ['hospital', 'organization', 'facility']):
        return 'organization', None
    if any(word in query_lower for word in ['insurance', 'payer']):
        return 'payer', None
    
    return None, None

def fuzzy_match_score(query, text, threshold=60):
    """Calculate fuzzy match score between query and text"""
    token_set_score = fuzz.token_set_ratio(query.lower(), text.lower())
    partial_score = fuzz.partial_ratio(query.lower(), text.lower())
    combined_score = (token_set_score * 0.7) + (partial_score * 0.3)
    return combined_score if combined_score >= threshold else 0

# ========================================
# RETRIEVER CLASS
# ========================================

class UltimateHybridRetriever:
    """Advanced retriever combining all strategies"""
    
    def __init__(self, vectorstore, actor_type=None, actor_id=None):
        self.vectorstore = vectorstore
        self.actor_type = actor_type
        self.actor_id = actor_id
    
    def get_relevant_documents(self, query):
        """Retrieve documents using ALL available strategies"""
        all_docs = []
        seen_content = set()
        
        expanded = expand_query(query)
        
        # Strategy 1: Semantic search
        try:
            if self.actor_type and self.actor_id:
                filter_key = f"{self.actor_type}_id"
                docs1 = self.vectorstore.similarity_search(
                    expanded, k=150, filter={filter_key: str(self.actor_id)}
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
        
        # Strategy 2: MMR for diversity
        try:
            if self.actor_type and self.actor_id:
                filter_key = f"{self.actor_type}_id"
                docs2 = self.vectorstore.max_marginal_relevance_search(
                    query, k=100, fetch_k=300, filter={filter_key: str(self.actor_id)}
                )
            else:
                docs2 = self.vectorstore.max_marginal_relevance_search(
                    query, k=50, fetch_k=150
                )
            
            for doc in docs2:
                content_hash = hash(doc.page_content)
                if content_hash not in seen_content:
                    seen_content.add(content_hash)
                    all_docs.append(doc)
        except Exception as e:
            print(f"⚠️ Strategy 2 failed: {e}")
        
        # Strategy 3: Keyword-based filtering
        if self.actor_type and self.actor_id:
            try:
                filter_key = f"{self.actor_type}_id"
                all_actor_docs = self.vectorstore.similarity_search(
                    "", k=1000, filter={filter_key: str(self.actor_id)}
                )
                
                keywords = [w for w in query.lower().split() if len(w) > 2]
                
                for doc in all_actor_docs:
                    content_lower = doc.page_content.lower()
                    if (any(kw in content_lower for kw in keywords) or 
                        doc.metadata.get("type") == "event"):
                        content_hash = hash(doc.page_content)
                        if content_hash not in seen_content:
                            seen_content.add(content_hash)
                            all_docs.append(doc)
            except Exception as e:
                print(f"⚠️ Strategy 3 failed: {e}")
        
        # Strategy 3.5: Fuzzy matching
        if self.actor_type and self.actor_id and len(all_docs) < 50:
            try:
                filter_key = f"{self.actor_type}_id"
                fuzzy_docs = self.vectorstore.similarity_search(
                    "", k=1000, filter={filter_key: str(self.actor_id)}
                )
                
                scored_docs = []
                for doc in fuzzy_docs:
                    score = fuzzy_match_score(query, doc.page_content, threshold=50)
                    if score > 0:
                        scored_docs.append((doc, score))
                
                scored_docs.sort(key=lambda x: x[1], reverse=True)
                for doc, score in scored_docs[:50]:
                    content_hash = hash(doc.page_content)
                    if content_hash not in seen_content:
                        seen_content.add(content_hash)
                        all_docs.append(doc)
            except Exception as e:
                print(f"⚠️ Fuzzy strategy failed: {e}")
        
        # Sort: prioritize event documents
        event_docs = [d for d in all_docs if d.metadata.get("type") == "event"]
        profile_docs = [d for d in all_docs if d.metadata.get("type") != "event"]
        
        # For non-actor queries, apply fuzzy ranking
        if not self.actor_type or not self.actor_id:
            scored_all = [(doc, fuzzy_match_score(query, doc.page_content, threshold=0)) 
                         for doc in all_docs]
            scored_all.sort(key=lambda x: x[1], reverse=True)
            return [doc for doc, score in scored_all]
        
        return event_docs + profile_docs

# ========================================
# PYDANTIC MODELS
# ========================================

class QueryRequest(BaseModel):
    query: str = Field(..., description="User's question about the healthcare data")
    max_results: Optional[int] = Field(100, description="Maximum number of documents to retrieve")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "What medications does patient 3 have?",
                "max_results": 100
            }
        }

class DocumentInfo(BaseModel):
    content: str
    metadata: Dict[str, Any]
    type: str

class QueryResponse(BaseModel):
    answer: str
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None
    num_documents_retrieved: int
    num_events: int
    documents: List[DocumentInfo]

class HealthResponse(BaseModel):
    status: str
    database_loaded: bool
    llm_connected: bool
    device: str

class DebugResponse(BaseModel):
    actor_type: str
    actor_id: str
    total_documents: int
    event_documents: int
    profile_documents: int
    sample_documents: List[DocumentInfo]

# ========================================
# LIFESPAN CONTEXT MANAGER
# ========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load resources on startup, cleanup on shutdown"""
    global vectorstore, llm, embedding_function
    
    print("🚀 Starting Healthcare RAG API...")
    
    # Detect device
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"📱 Using device: {DEVICE}")
    
    # Initialize embeddings
    print("📄 Loading embeddings...")
    embedding_function = HuggingFaceEmbeddings(
        model_name="BAAI/bge-base-en-v1.5",
        model_kwargs={'device': DEVICE}
    )
    
    # Load vectorstore
    DB_FOLDER = "faiss_index_ultimate_v6"
    print(f"💾 Loading FAISS index from {DB_FOLDER}...")
    
    try:
        vectorstore = FAISS.load_local(
            DB_FOLDER, 
            embedding_function, 
            allow_dangerous_deserialization=True
        )
        print("✅ FAISS index loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load FAISS index: {e}")
        print("⚠️ Please run the main script first to build the database!")
        vectorstore = None
    
    # Initialize LLM
    print("🔌 Connecting to Groq...")
    try:
        llm = ChatGroq(temperature=0, model_name="llama-3.3-70b-versatile")
        print("✅ LLM connected")
    except Exception as e:
        print(f"❌ Failed to connect to LLM: {e}")
        llm = None
    
    print("🎉 API Ready!\n")
    
    yield
    
    # Cleanup
    print("👋 Shutting down...")

# ========================================
# FASTAPI APP
# ========================================

app = FastAPI(
    title="Healthcare Data Warehouse RAG API",
    description="Advanced RAG system for querying healthcare data with multi-actor support",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================================
# API ENDPOINTS
# ========================================

@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "database_loaded": vectorstore is not None,
        "llm_connected": llm is not None,
        "device": "cuda" if torch.cuda.is_available() else "cpu"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Detailed health check"""
    return {
        "status": "online",
        "database_loaded": vectorstore is not None,
        "llm_connected": llm is not None,
        "device": "cuda" if torch.cuda.is_available() else "cpu"
    }

@app.post("/query", response_model=QueryResponse)
async def query_database(request: QueryRequest):
    """
    Query the healthcare data warehouse
    
    Examples:
    - "What medications does patient 3 have?"
    - "Tell me about provider 1"
    - "What services does organization 3 provide?"
    - "Who is covered by payer 2?"
    """
    
    if vectorstore is None:
        raise HTTPException(
            status_code=503, 
            detail="Database not loaded. Please build the FAISS index first."
        )
    
    if llm is None:
        raise HTTPException(
            status_code=503, 
            detail="LLM not connected. Check your GROQ_API_KEY."
        )
    
    try:
        # Detect actor type and ID
        actor_type, actor_id = detect_query_actor(request.query)
        
        # Retrieve documents
        retriever = UltimateHybridRetriever(vectorstore, actor_type, actor_id)
        retrieved_docs = retriever.get_relevant_documents(request.query)
        
        if len(retrieved_docs) == 0:
            return QueryResponse(
                answer="I don't know - no relevant documents found.",
                actor_type=actor_type,
                actor_id=actor_id,
                num_documents_retrieved=0,
                num_events=0,
                documents=[]
            )
        
        # Build context
        context = "\n\n".join([doc.page_content for doc in retrieved_docs[:request.max_results]])
        
        # Create prompt
        template = """You are a Healthcare Data Warehouse Analyst with comprehensive access to patient records.
Answer using ONLY the provided context below.
If information is not in the context, say "I don't know".

IMPORTANT INSTRUCTIONS:
1. The context contains multiple document chunks - read ALL of them carefully
2. Event documents contain detailed information about specific medical events
3. Profile documents provide summary statistics and demographics
4. Provide specific details from events when available, not just summaries

Context:
{context}

Question: {question}

Answer (be specific, detailed, and use information from the context):
"""
        
        prompt = PromptTemplate(template=template, input_variables=["context", "question"])
        messages = prompt.format(context=context, question=request.query)
        
        # Get LLM response
        response = llm.invoke(messages)
        
        # Prepare document info
        documents_info = []
        for doc in retrieved_docs[:20]:  # Return first 20 docs in response
            documents_info.append(DocumentInfo(
                content=doc.page_content[:500],  # Truncate for API response
                metadata=doc.metadata,
                type=doc.metadata.get("type", "unknown")
            ))
        
        # Count events
        num_events = len([d for d in retrieved_docs[:request.max_results] 
                         if d.metadata.get("type") == "event"])
        
        return QueryResponse(
            answer=response.content,
            actor_type=actor_type,
            actor_id=actor_id,
            num_documents_retrieved=len(retrieved_docs),
            num_events=num_events,
            documents=documents_info
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/debug/{actor_type}/{actor_id}", response_model=DebugResponse)
async def debug_actor(
    actor_type: str,
    actor_id: str
):
    """
    Debug endpoint to see all documents for a specific actor
    
    Example: GET /debug/patient/3
    
    Args:
        actor_type: Actor type (patient, provider, organization, or payer)
        actor_id: Actor ID number
    """
    
    if vectorstore is None:
        raise HTTPException(status_code=503, detail="Database not loaded")
    
    try:
        filter_key = f"{actor_type}_id"
        debug_docs = vectorstore.similarity_search(
            "", k=1000, filter={filter_key: actor_id}
        )
        
        event_docs = [d for d in debug_docs if d.metadata.get("type") == "event"]
        profile_docs = [d for d in debug_docs if d.metadata.get("type") != "event"]
        
        # Get sample documents
        sample_docs = []
        for doc in debug_docs[:5]:
            sample_docs.append(DocumentInfo(
                content=doc.page_content[:200],
                metadata=doc.metadata,
                type=doc.metadata.get("type", "unknown")
            ))
        
        return DebugResponse(
            actor_type=actor_type,
            actor_id=actor_id,
            total_documents=len(debug_docs),
            event_documents=len(event_docs),
            profile_documents=len(profile_docs),
            sample_documents=sample_docs
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")

@app.get("/stats")
async def get_stats():
    """Get database statistics"""
    
    if vectorstore is None:
        raise HTTPException(status_code=503, detail="Database not loaded")
    
    try:
        # Sample queries to get counts
        all_docs = vectorstore.similarity_search("", k=10000)
        
        patients = set()
        providers = set()
        organizations = set()
        payers = set()
        events = 0
        
        for doc in all_docs:
            metadata = doc.metadata
            if "patient_id" in metadata:
                patients.add(metadata["patient_id"])
            if "provider_id" in metadata:
                providers.add(metadata["provider_id"])
            if "organization_id" in metadata:
                organizations.add(metadata["organization_id"])
            if "payer_id" in metadata:
                payers.add(metadata["payer_id"])
            if metadata.get("type") == "event":
                events += 1
        
        return {
            "total_documents": len(all_docs),
            "unique_patients": len(patients),
            "unique_providers": len(providers),
            "unique_organizations": len(organizations),
            "unique_payers": len(payers),
            "total_events": events
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats failed: {str(e)}")

# ========================================
# RUN SERVER
# ========================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)