import pandas as pd
import json
from faker import Faker
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from datetime import datetime
from pathlib import Path

# === Setup with French locale ===
fake = Faker('fr_FR')
base_dir = Path('synthea-morocco\output') 
output_dir = Path("moroccan_unstructured_data")
output_dir.mkdir(exist_ok=True)

# Create subdirectories
pdf_dir = output_dir / "pdf"
txt_dir = output_dir / "txt"
pdf_dir.mkdir(exist_ok=True)
txt_dir.mkdir(exist_ok=True)

# === Load all CSV tables ===
def load_csv(name):
    path = base_dir / f"{name}.csv"
    if path.exists():
        try:
            return pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"Avertissement: Ignore {name}, impossible à lire: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

tables = {name: load_csv(name) for name in [
    "patients", "encounters", "conditions", "observations", "medications",
    "procedures", "immunizations", "imaging_studies", "careplans",
    "allergies", "supplies", "payer_transitions", "payers", "providers",
    "organizations"
]}

# === French month names ===
FRENCH_MONTHS = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril",
    5: "mai", 6: "juin", 7: "juillet", 8: "août",
    9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}

# === Initialize PDF with custom styles ===
styles = getSampleStyleSheet()

# Header style
styles.add(ParagraphStyle(
    name='Header',
    parent=styles['Heading1'],
    fontSize=18,
    textColor=colors.HexColor('#1a4d7a'),
    spaceAfter=12,
    alignment=TA_CENTER,
    fontName='Helvetica-Bold'
))

# Section header style
styles.add(ParagraphStyle(
    name='SectionHeader',
    parent=styles['Heading2'],
    fontSize=12,
    textColor=colors.HexColor('#2c5f7c'),
    spaceBefore=12,
    spaceAfter=8,
    fontName='Helvetica-Bold',
    backColor=colors.HexColor('#e8f0f5'),
    leftIndent=5,
    rightIndent=5
))

# Patient info style
styles.add(ParagraphStyle(
    name='PatientInfo',
    parent=styles['Normal'],
    fontSize=10,
    leading=14,
    fontName='Helvetica'
))

# Medical data style
styles.add(ParagraphStyle(
    name='MedicalData',
    parent=styles['Normal'],
    fontSize=9,
    leading=12,
    leftIndent=20,
    fontName='Helvetica'
))

# Doctor notes style
styles.add(ParagraphStyle(
    name='DoctorNotes',
    parent=styles['Normal'],
    fontSize=10,
    leading=14,
    leftIndent=15,
    fontName='Helvetica-Oblique',
    textColor=colors.HexColor('#555555')
))

# Footer style
styles.add(ParagraphStyle(
    name='Footer',
    parent=styles['Normal'],
    fontSize=8,
    textColor=colors.grey,
    alignment=TA_CENTER
))

def format_currency_mad(amount):
    """Format number as Moroccan Dirham"""
    try:
        value = float(amount)
    except Exception:
        return "N/A"
    return f"{value:,.0f} MAD".replace(",", " ")

def random_variation(value, pct=0.15):
    """Return value with ±pct random variation"""
    try:
        base = float(value)
    except Exception:
        return value
    delta = base * pct
    return max(0, base + fake.pyfloat(min_value=-delta, max_value=delta))

def pick_first_numeric_column(df, candidates):
    """Return first existing numeric column name from candidates, or None"""
    if df is None or df.empty:
        return None
    for col in candidates:
        if col in df.columns:
            try:
                pd.to_numeric(df[col], errors='coerce')
                return col
            except Exception:
                continue
    return None

def estimate_costs(rel):
    """Compute per-encounter costs and total estimated cost in MAD.
    Prefer existing numeric columns; otherwise estimate based on simple heuristics."""
    total_cost = 0.0
    encounter_costs = []

    enc_df = rel.get("encounters", pd.DataFrame())
    proc_df = rel.get("procedures", pd.DataFrame())
    img_df = rel.get("imaging_studies", pd.DataFrame())
    obs_df = rel.get("observations", pd.DataFrame())

    # Try to find cost columns
    enc_cost_col = pick_first_numeric_column(enc_df, [
        "TOTAL_COST", "TOTALCOST", "COST", "BASE_COST", "BASECOST", "AMOUNT", "PRICE"
    ])
    proc_cost_col = pick_first_numeric_column(proc_df, [
        "TOTAL_COST", "TOTALCOST", "COST", "BASE_COST", "BASECOST", "AMOUNT", "PRICE"
    ])

    if not enc_df.empty:
        # Build encounter-level costs
        for _, e in enc_df.head(5).iterrows():
            # Prefer encounter cost, else infer from procedures, else heuristic
            cost = None
            if enc_cost_col:
                cost = pd.to_numeric(e.get(enc_cost_col, None), errors='coerce')
            if (cost is None or pd.isna(cost)) and not proc_df.empty and "ENCOUNTER" in proc_df.columns and "id" in enc_df.columns:
                # Sum procedures linked to this encounter
                linked = proc_df[proc_df.get("ENCOUNTER", "") == e.get("id", "")]
                if not linked.empty and proc_cost_col:
                    cost = pd.to_numeric(linked[proc_cost_col], errors='coerce').fillna(0).sum()
            if cost is None or pd.isna(cost):
                # Heuristic by class
                enc_class = str(e.get("ENCOUNTERCLASS", "outpatient")).lower()
                if enc_class == "inpatient":
                    base = 3500 + fake.random_int(min=0, max=4500)
                elif enc_class == "emergency":
                    base = 1200 + fake.random_int(min=0, max=1800)
                else:
                    base = 250 + fake.random_int(min=0, max=550)
                # Add extras if imaging/procedures/complex obs present
                extras = 0
                if not proc_df.empty and "ENCOUNTER" in proc_df.columns and e.get("id", None):
                    extras += 150 * len(proc_df[proc_df["ENCOUNTER"] == e["id"]].head(3))
                if not img_df.empty and "ENCOUNTER" in img_df.columns and e.get("id", None):
                    extras += 300 * len(img_df[img_df["ENCOUNTER"] == e["id"]].head(2))
                if not obs_df.empty and "ENCOUNTER" in obs_df.columns and e.get("id", None):
                    extras += 20 * len(obs_df[obs_df["ENCOUNTER"] == e["id"]].head(5))
                cost = random_variation(base + extras, pct=0.08)
            # Accumulate
            try:
                cost_float = float(cost)
            except Exception:
                cost_float = 0.0
            total_cost += cost_float
            encounter_costs.append((e, cost_float))

    # If there are procedures without encounters, lightly add their costs
    if not proc_df.empty and not proc_cost_col and not enc_df.empty:
        # Already estimated via encounters
        pass
    elif not proc_df.empty and proc_cost_col and (enc_df.empty or not encounter_costs):
        # Use procedure totals if encounters missing
        proc_total = pd.to_numeric(proc_df[proc_cost_col], errors='coerce').fillna(0).sum()
        total_cost = float(proc_total)

    return encounter_costs, total_cost

def format_date_french(date_str):
    """Format date string to readable French format"""
    if pd.isna(date_str) or date_str == '' or date_str == 'Unknown':
        return 'N/A'
    try:
        date_str_clean = str(date_str).strip()
        # Try parsing ISO format with time
        if 'T' in date_str_clean:
            if '+' in date_str_clean:
                date_str_clean = date_str_clean.split('+')[0]
            elif 'Z' in date_str_clean:
                date_str_clean = date_str_clean.replace('Z', '')
            if '.' in date_str_clean:
                dt = datetime.strptime(date_str_clean.split('.')[0], '%Y-%m-%dT%H:%M:%S')
            else:
                dt = datetime.strptime(date_str_clean, '%Y-%m-%dT%H:%M:%S')
        else:
            dt = datetime.strptime(date_str_clean, '%Y-%m-%d')
        return f"{dt.day} {FRENCH_MONTHS[dt.month]} {dt.year}"
    except Exception as e:
        return str(date_str)[:20]

def format_date_short_french(date_str):
    """Format date to short French format"""
    if pd.isna(date_str) or date_str == '' or date_str == 'Unknown':
        return 'N/A'
    try:
        date_str_clean = str(date_str).strip()
        if 'T' in date_str_clean:
            if '+' in date_str_clean:
                date_str_clean = date_str_clean.split('+')[0]
            elif 'Z' in date_str_clean:
                date_str_clean = date_str_clean.replace('Z', '')
            if '.' in date_str_clean:
                dt = datetime.strptime(date_str_clean.split('.')[0], '%Y-%m-%dT%H:%M:%S')
            else:
                dt = datetime.strptime(date_str_clean, '%Y-%m-%dT%H:%M:%S')
        else:
            dt = datetime.strptime(date_str_clean, '%Y-%m-%d')
        return dt.strftime('%d/%m/%Y')
    except Exception as e:
        return str(date_str)[:10]

def calculate_age(birthdate_str):
    """Calculate age from birthdate"""
    if pd.isna(birthdate_str) or birthdate_str == '' or birthdate_str == 'Unknown':
        return 'N/A'
    try:
        date_str_clean = str(birthdate_str).strip()
        if 'T' in date_str_clean:
            if '+' in date_str_clean:
                date_str_clean = date_str_clean.split('+')[0]
            elif 'Z' in date_str_clean:
                date_str_clean = date_str_clean.replace('Z', '')
            if '.' in date_str_clean:
                birth = datetime.strptime(date_str_clean.split('.')[0], '%Y-%m-%dT%H:%M:%S')
            else:
                birth = datetime.strptime(date_str_clean, '%Y-%m-%dT%H:%M:%S')
        else:
            birth = datetime.strptime(date_str_clean, '%Y-%m-%d')
        today = datetime.now()
        age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
        return str(age)
    except Exception as e:
        return 'N/A'

def generate_clinical_notes_french(rel, patient_name, age, gender):
    """Generate realistic clinical notes in French based on patient data"""
    notes_parts = []
    gender_term = "masculin" if gender == "M" else "féminin"
    age_phrase = f"âgé(e) de {age} ans" if age != "N/A" else ""
    opening = fake.random_element(elements=[
        f"Patient(e) {age_phrase} de sexe {gender_term}, venu(e) pour un contrôle clinique.",
        f"{age_phrase.capitalize()} de sexe {gender_term}, consultation programmée pour suivi global.",
        f"Consultation de routine chez un(e) patient(e) {gender_term} {age_phrase}."
    ])
    notes_parts.append(opening)

    # Conditions and risk
    cond_list = []
    if "conditions" in rel and not rel["conditions"].empty:
        for _, r in rel["conditions"].head(5).iterrows():
            d = r.get('DESCRIPTION', '')
            if d:
                cond_list.append(d)
    if cond_list:
        notes_parts.append(f"Antécédents: {', '.join(sorted(set(cond_list))[:4])}.")
    else:
        notes_parts.append("Pas d'antécédents pathologiques notables documentés.")

    # Symptoms and review of systems
    ros_phrases = [
        "Fatigue modérée à l'effort, pas de dyspnée au repos.",
        "Pas de douleur thoracique, pas de palpitations signalées.",
        "Sommeil globalement correct, appétit conservé.",
        "Pas de signe d'infection active (fièvre, toux productive)."
    ]
    notes_parts.append(fake.random_element(ros_phrases))

    # Medications
    if "medications" in rel and not rel["medications"].empty:
        active_meds, past_meds = [], []
        for _, r in rel["medications"].head(8).iterrows():
            med_name = r.get('DESCRIPTION', '')
            stop_date = r.get('STOP', '')
            if med_name:
                if pd.isna(stop_date) or str(stop_date).strip() == '':
                    active_meds.append(med_name)
                else:
                    past_meds.append(med_name)
        if active_meds:
            notes_parts.append(f"Traitements en cours: {', '.join(sorted(set(active_meds))[:4])}.")
        if past_meds:
            notes_parts.append(f"Historique de traitements arrêtés: {', '.join(sorted(set(past_meds))[:3])}.")

    # Observations summary
    if "observations" in rel and not rel["observations"].empty:
        recent_obs = rel["observations"].head(5)
        vital_signs = []
        for _, r in recent_obs.iterrows():
            desc = r.get('DESCRIPTION', '')
            value = r.get('VALUE', '')
            units = r.get('UNITS', '')
            if desc and value:
                vital_signs.append(f"{desc} {value} {units}".strip())
        if vital_signs:
            notes_parts.append(f"Dernières mesures pertinentes: {'; '.join(vital_signs[:4])}.")

    # Plan
    plan_lines = [
        "Poursuivre le traitement actuel, adapter si pression artérielle instable.",
        "Recommander un bilan biologique (glycémie à jeun, bilan lipidique).",
        "Conseiller activité physique adaptée et régime pauvre en sel.",
        "ECG à envisager si symptomatologie cardiaque persistante."
    ]
    notes_parts.append("Plan: " + " ".join(fake.random_sample(elements=plan_lines, length=2)))

    # Safety net
    notes_parts.append("Informer le/la patient(e) de consulter en urgence en cas de douleur thoracique, dyspnée aiguë ou signes neurologiques.")

    return " ".join(notes_parts)

def synthesize_conditions(age_value, existing_descriptions):
    """Add plausible chronic conditions if the patient's list is sparse."""
    try:
        age_num = int(age_value)
    except Exception:
        age_num = None
    suggestions = ["Ostéoporose", "Arthrose", "Diabète de type 2", "Insuffisance cardiaque", "Maladie rénale chronique"]
    add = []
    if age_num is not None and age_num >= 70:
        for s in suggestions:
            if not any(s.lower() in str(x).lower() for x in existing_descriptions):
                add.append(s)
            if len(add) >= 2:
                break
    return add

def synthesize_immunizations_if_missing(rel):
    """Return a small DataFrame of synthetic immunizations if none exist."""
    if "immunizations" in rel and not rel["immunizations"].empty:
        return None
    rows = []
    today = datetime.now()
    choices = [
        ("Vaccin contre la grippe", today.replace(year=today.year - 1)),
        ("Vaccin COVID-19 (rappel)", today.replace(year=today.year - 1, month=max(1, today.month - 4))),
        ("Vaccin tétanos (rappel)", today.replace(year=today.year - 5)),
    ]
    for label, dt in choices[:fake.random_int(min=1, max=3)]:
        rows.append({"DESCRIPTION": label, "DATE": dt.strftime("%Y-%m-%d")})
    return pd.DataFrame(rows)

def create_header(org_name=None):
    """Create report header with hospital/clinic information in French"""
    if org_name:
        header_title = org_name.upper()
    else:
        header_title = "SYSTÈME DE SANTÉ MAROCAIN"
    
    try:
        now = datetime.now()
        report_time = f"{now.day} {FRENCH_MONTHS[now.month]} {now.year} à {now.strftime('%H:%M')}"
    except:
        report_time = datetime.now().strftime("%d/%m/%Y")
    
    header_data = [
        [header_title, ''],
        ['DOSSIER MÉDICAL COMPLET', f'Date du rapport: {report_time}'],
        ['', 'Document médical confidentiel']
    ]
    header_table = Table(header_data, colWidths=[4.5*inch, 2.5*inch])
    header_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#1a4d7a')),
        ('TEXTCOLOR', (0, 0), (0, 0), colors.whitesmoke),
        ('TEXTCOLOR', (0, 1), (0, 1), colors.HexColor('#1a4d7a')),
        ('TEXTCOLOR', (1, 1), (1, 2), colors.HexColor('#666666')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (1, 1), (1, 2), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 14),
        ('FONTNAME', (0, 1), (0, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 1), (0, 1), 11),
        ('FONTNAME', (1, 1), (1, 1), 'Helvetica'),
        ('FONTSIZE', (1, 1), (1, 1), 9),
        ('FONTNAME', (1, 2), (1, 2), 'Helvetica-Oblique'),
        ('FONTSIZE', (1, 2), (1, 2), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('LINEBELOW', (0, 2), (-1, 2), 2, colors.HexColor('#1a4d7a')),
    ]))
    return header_table

def create_patient_info_table(name, gender, birth, address, age, pid):
    """Create patient information table in French"""
    gender_fr = "Masculin" if gender == "M" else "Féminin"
    patient_data = [
        ['INFORMATIONS PATIENT', ''],
        ['Nom du patient:', name],
        ['ID Patient:', str(pid)[:36]],
        ['Date de naissance:', format_date_french(birth)],
        ['Âge:', f"{age} ans" if age != "N/A" else "N/A"],
        ['Sexe:', gender_fr],
        ['Adresse:', address]
    ]
    patient_table = Table(patient_data, colWidths=[2*inch, 5*inch])
    patient_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c5f7c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 1), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    return patient_table

def add_page_number(canvas, doc):
    """Add page number to footer"""
    page_num = canvas.getPageNumber()
    text = f"Page {page_num}"
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    canvas.setFillColor(colors.grey)
    canvas.drawRightString(7.5*inch, 0.5*inch, text)
    canvas.restoreState()

# === Generate reports for 50 patients ===
if tables["patients"].empty:
    print("Erreur: Aucun patient trouvé dans patients.csv")
    exit(1)

# Select unique patients (remove duplicates) — keep stable order, and restrict to those with encounters
patients_df = tables["patients"].drop_duplicates(subset=['id'])
if not tables.get("encounters", pd.DataFrame()).empty and "PATIENT" in tables["encounters"].columns:
    encounter_patients = set(tables["encounters"]["PATIENT"].dropna().unique().tolist())
    patients_df = patients_df[patients_df["id"].isin(encounter_patients)]
total_patients = len(patients_df)
print(f"Traitement de {total_patients} patients uniques...")

patient_count = 0
pdf_count = 0
max_pdfs = 50

# Pre-group related tables by PATIENT for faster access
grouped_by_patient = {}
for k, v in tables.items():
    if not v.empty and "PATIENT" in v.columns:
        try:
            grouped_by_patient[k] = v.groupby("PATIENT")
        except Exception:
            grouped_by_patient[k] = None
    else:
        grouped_by_patient[k] = None

for _, p in patients_df.iterrows():
    patient_count += 1
    print(f"   Traitement du patient {patient_count}/{total_patients}...")
    
    pid = p["id"]
    name = f"{p.get('FIRST', '')} {p.get('LAST', '')}".strip()
    gender = p.get("GENDER", "Unknown")
    birth = p.get("BIRTHDATE", "Unknown")
    address = p.get("ADDRESS", "Unknown")
    age = calculate_age(birth)
    
    # Get physician name (French)
    physician_name = f"Dr. {fake.last_name()}"

    # Related tables - fast lookup by patient ID
    rel = {}
    for k, gb in grouped_by_patient.items():
        if gb is not None:
            try:
                rel[k] = gb.get_group(pid).copy()
            except Exception:
                rel[k] = pd.DataFrame()
        else:
            rel[k] = pd.DataFrame()

    # Create content for this patient
    content = []
    
    # Get organization name
    org_name = None
    if "encounters" in rel and not rel["encounters"].empty:
        first_enc = rel["encounters"].iloc[0]
        org_id = first_enc.get('ORGANIZATION', '')
        if org_id and not tables["organizations"].empty:
            org_row = tables["organizations"][tables["organizations"]["id"] == org_id]
            if not org_row.empty:
                org_name = org_row.iloc[0].get("NAME", None)
    
    # Header
    content.append(create_header(org_name))
    content.append(Spacer(1, 0.2*inch))
    
    # Patient Information
    content.append(create_patient_info_table(name, gender, birth, address, age, pid))
    content.append(Spacer(1, 0.3*inch))
    
    # Executive Summary
    summary_items = []
    if "conditions" in rel and not rel["conditions"].empty:
        active_conditions = len(rel["conditions"])
        summary_items.append(f"{active_conditions} condition(s) active(s)")
    if "medications" in rel and not rel["medications"].empty:
        active_meds_count = sum(1 for _, r in rel["medications"].iterrows() if pd.isna(r.get('STOP', '')) or r.get('STOP', '') == '')
        if active_meds_count > 0:
            summary_items.append(f"{active_meds_count} médicament(s) actif(s)")
    if "encounters" in rel and not rel["encounters"].empty:
        encounter_count = len(rel["encounters"])
        summary_items.append(f"{encounter_count} consultation(s) enregistrée(s)")
    
    if summary_items:
        summary_text = "Résumé du patient: " + "; ".join(summary_items) + "."
        summary_style = ParagraphStyle(
            name='Summary',
            parent=styles['Normal'],
            fontSize=10,
            leading=14,
            fontName='Helvetica-Bold',
            textColor=colors.HexColor('#1a4d7a'),
            backColor=colors.HexColor('#f0f7ff'),
            leftIndent=10,
            rightIndent=10,
            spaceBefore=6,
            spaceAfter=6
        )
        content.append(Paragraph(summary_text, summary_style))
        content.append(Spacer(1, 0.2*inch))

    # Provider & Organization overview block
    provider_overview_rows = [['Prestataire de soins', 'Informations']]
    provider_overview_rows.append(['Médecin traitant', 'Non disponible'])
    if "encounters" in rel and not rel["encounters"].empty:
        # Provider
        prov_name = None
        prov_spec = None
        provider_id = rel["encounters"].iloc[0].get('PROVIDER', '')
        if provider_id and not tables["providers"].empty:
            prov_row = tables["providers"][tables["providers"]["id"] == provider_id]
            if not prov_row.empty:
                prov_name = prov_row.iloc[0].get('NAME', None)
                prov_spec = prov_row.iloc[0].get('SPECIALTY', None)
        if prov_name:
            if not prov_name.startswith('Dr.') and not prov_name.startswith('Dr '):
                prov_name = f"Dr. {prov_name}"
            provider_overview_rows[-1][1] = prov_name + (f" — {prov_spec}" if prov_spec else "")
        # Organization
        org_display = org_name if org_name else "Inconnu"
        provider_overview_rows.append(['Établissement principal', org_display])

    overview_table = Table(provider_overview_rows, colWidths=[2.5*inch, 4.5*inch])
    overview_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dfeaf4')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1a4d7a')),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e2e2e2')),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    content.append(overview_table)
    content.append(Spacer(1, 0.2*inch))

    # Préparer les données pour une utilisation cohérente PDF + TXT
    allergies_entries = []
    if "allergies" in rel and not rel["allergies"].empty:
        for _, r in rel["allergies"].head(5).iterrows():
            allergies_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                format_date_short_french(r.get('START', 'N/A'))
            ))

    conditions_entries = []
    if "conditions" in rel and not rel["conditions"].empty:
        cond_df = rel["conditions"].head(10).copy()
        existing_desc = [r.get('DESCRIPTION', 'Inconnu') for _, r in cond_df.iterrows()]
        synth_add = synthesize_conditions(age, existing_desc) if len(existing_desc) < 2 else []
        for _, r in cond_df.iterrows():
            conditions_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                format_date_short_french(r.get('START', 'N/A'))
            ))
        for desc in synth_add:
            conditions_entries.append((desc, "N/A"))

    medications_entries = []
    if "medications" in rel and not rel["medications"].empty:
        for _, r in rel["medications"].head(10).iterrows():
            stop_val = r.get('STOP', '')
            if pd.isna(stop_val) or stop_val == '':
                stop_text = 'En cours'
                status = 'Actif'
            else:
                stop_text = format_date_short_french(stop_val)
                status = 'Terminé'
            medications_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                format_date_short_french(r.get('START', 'N/A')),
                stop_text,
                status
            ))

    observations_entries = []
    if "observations" in rel and not rel["observations"].empty:
        for _, r in rel["observations"].head(8).iterrows():
            observations_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                str(r.get('VALUE', 'N/A')) if r.get('VALUE', '') != '' else 'N/A',
                r.get('UNITS', '') if r.get('UNITS', '') else 'N/A',
                format_date_short_french(r.get('DATE', 'N/A'))
            ))
    if len(observations_entries) <= 2:
        today_txt = datetime.now().strftime("%d/%m/%Y")
        synth_obs = [
            ("Glycémie à jeun", f"{fake.random_int(min=85, max=115)}", "mg/dL"),
            ("Cholestérol total", f"{fake.random_int(min=160, max=240)}", "mg/dL"),
            ("Poids", f"{fake.random_int(min=50, max=90)}", "kg"),
        ]
        for label, val, unit in synth_obs[:2]:
            observations_entries.append((label, val, unit, today_txt))

    procedures_entries = []
    if "procedures" in rel and not rel["procedures"].empty:
        for _, r in rel["procedures"].head(8).iterrows():
            procedures_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                format_date_french(r.get('DATE', 'N/A'))
            ))

    immunizations_entries = []
    if "immunizations" in rel and not rel["immunizations"].empty:
        for _, r in rel["immunizations"].head(5).iterrows():
            immunizations_entries.append((
                r.get('DESCRIPTION', 'Inconnu'),
                format_date_french(r.get('DATE', 'N/A'))
            ))
    else:
        imm_df_synth = synthesize_immunizations_if_missing(rel)
        if imm_df_synth is not None and not imm_df_synth.empty:
            for _, r in imm_df_synth.iterrows():
                immunizations_entries.append((
                    r.get('DESCRIPTION', 'Inconnu'),
                    format_date_french(r.get('DATE', 'N/A'))
                ))

    imaging_entries = []
    if "imaging_studies" in rel and not rel["imaging_studies"].empty:
        for _, r in rel["imaging_studies"].head(5).iterrows():
            imaging_entries.append((
                format_date_french(r.get('DATE', 'N/A')),
                str(r.get('MODALITY_CODE', 'Inconnu')),
                str(r.get('BODYSITE_CODE', 'N/A'))
            ))

    encounter_costs, total_cost_mad = estimate_costs(rel)
    encounters_entries = []
    if "encounters" in rel and not rel["encounters"].empty:
        for idx, (_, e) in enumerate(rel["encounters"].head(5).iterrows()):
            org_id = e.get('ORGANIZATION', '')
            org_label = "Inconnu"
            if org_id and not tables["organizations"].empty:
                org_row = tables["organizations"][tables["organizations"]["id"] == org_id]
                if not org_row.empty:
                    org_label = org_row.iloc[0].get("NAME", org_id)
            amount_str = "N/A"
            if idx < len(encounter_costs):
                amount_str = format_currency_mad(encounter_costs[idx][1])
            encounters_entries.append((
                format_date_french(e.get('START', 'N/A')),
                org_label[:60],
                e.get('ENCOUNTERCLASS', 'N/A'),
                amount_str,
                e.get('CODE', 'raison non spécifiée')
            ))

    lifestyle_text = fake.random_element([
        "Non fumeur, alcool: occasionnel.",
        "Ancien fumeur, sevré depuis plus de 10 ans.",
        "Jamais fumeur, pas de consommation d'alcool.",
    ])
    family_text = fake.random_element([
        "Antécédents familiaux: HTA chez le père, diabète chez la mère.",
        "Antécédents familiaux: sans particularité notable.",
        "Antécédents familiaux: cardiopathie ischémique chez un frère."
    ])

    # Allergies Section
    if allergies_entries:
        content.append(Paragraph("ALLERGIES", styles['SectionHeader']))
        allergy_data = [['Allergène', 'Date identifiée']]
        for desc, date_txt in allergies_entries:
            allergy_data.append([desc, date_txt])
        allergy_table = Table(allergy_data, colWidths=[4*inch, 3*inch])
        allergy_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dc3545')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff5f5')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        content.append(allergy_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("ALLERGIES", styles['SectionHeader']))
        content.append(Paragraph("Aucune allergie connue", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Conditions Section
    if conditions_entries:
        content.append(Paragraph("CONDITIONS ACTIVES", styles['SectionHeader']))
        condition_data = [['Condition', 'Date de diagnostic']]
        for desc, date_txt in conditions_entries:
            condition_data.append([desc, date_txt])
        condition_table = Table(condition_data, colWidths=[4.5*inch, 2.5*inch])
        condition_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c5f7c')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f7fa')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        content.append(condition_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("CONDITIONS ACTIVES", styles['SectionHeader']))
        content.append(Paragraph("Aucune condition active documentée", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Medications Section
    if medications_entries:
        content.append(Paragraph("MÉDICAMENTS ACTUELS", styles['SectionHeader']))
        medication_data = [['Médicament', 'Date début', 'Date fin', 'Statut']]
        for entry in medications_entries:
            medication_data.append(list(entry))
        medication_table = Table(medication_data, colWidths=[3*inch, 1.5*inch, 1.5*inch, 1*inch])
        medication_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#28a745')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 1), (3, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0fff4')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(medication_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("MÉDICAMENTS ACTUELS", styles['SectionHeader']))
        content.append(Paragraph("Aucun médicament actif documenté", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Observations Section
    if observations_entries:
        content.append(Paragraph("OBSERVATIONS RÉCENTES & SIGNES VITAUX", styles['SectionHeader']))
        obs_data = [['Observation', 'Valeur', 'Unités', 'Date']]
        for entry in observations_entries:
            obs_data.append(list(entry))
        obs_table = Table(obs_data, colWidths=[2.5*inch, 1.5*inch, 1*inch, 1.5*inch])
        obs_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#17a2b8')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 1), (3, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#e7f3f5')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(obs_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("OBSERVATIONS RÉCENTES & SIGNES VITAUX", styles['SectionHeader']))
        content.append(Paragraph("Aucune observation récente", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Procedures Section
    if procedures_entries:
        content.append(Paragraph("PROCÉDURES RÉCENTES", styles['SectionHeader']))
        proc_data = [['Procédure', 'Date']]
        for desc, date_txt in procedures_entries:
            proc_data.append([desc, date_txt])
        proc_table = Table(proc_data, colWidths=[5*inch, 2*inch])
        proc_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#6c757d')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 1), (1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(proc_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("PROCÉDURES RÉCENTES", styles['SectionHeader']))
        content.append(Paragraph("Aucune procédure récente documentée", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Immunizations Section
    if immunizations_entries:
        content.append(Paragraph("VACCINATIONS", styles['SectionHeader']))
        imm_data = [['Vaccin', 'Date']]
        for desc, date_txt in immunizations_entries:
            imm_data.append([desc, date_txt])
        imm_table = Table(imm_data, colWidths=[5*inch, 2*inch])
        imm_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ffc107')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 1), (1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fffbf0')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(imm_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("VACCINATIONS", styles['SectionHeader']))
        content.append(Paragraph("Aucune vaccination récente documentée", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Imaging Studies Section
    if imaging_entries:
        content.append(Paragraph("ÉTUDES D'IMAGERIE", styles['SectionHeader']))
        img_data = [['Date de l\'étude', 'Modalité', 'Site corporel']]
        for entry in imaging_entries:
            img_data.append(list(entry))
        img_table = Table(img_data, colWidths=[2.5*inch, 2*inch, 2.5*inch])
        img_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#6610f2')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f0ff')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(img_table)
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("ÉTUDES D'IMAGERIE", styles['SectionHeader']))
        content.append(Paragraph("Aucune étude d'imagerie disponible", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Encounters Section (with costs)
    if encounters_entries:
        content.append(Paragraph("CONSULTATIONS RÉCENTES", styles['SectionHeader']))
        enc_data = [['Date', 'Organisation', 'Type', 'Montant']]
        for date_txt, org, enc_class, amount_txt, _ in encounters_entries:
            enc_data.append([date_txt, org, enc_class, amount_txt])
        enc_table = Table(enc_data, colWidths=[1.7*inch, 3.0*inch, 1.3*inch, 1.0*inch])
        enc_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#fd7e14')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (3, 1), (3, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff5f0')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        content.append(enc_table)
        content.append(Spacer(1, 0.2*inch))

        billing_text = f"Montant total estimé des consultations: {format_currency_mad(total_cost_mad)} (incluant consultation, actes et examens éventuels)"
        content.append(Paragraph(billing_text, styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))
    else:
        content.append(Paragraph("CONSULTATIONS RÉCENTES", styles['SectionHeader']))
        content.append(Paragraph("Aucune consultation enregistrée", styles['MedicalData']))
        content.append(Spacer(1, 0.2*inch))

    # Social and family history section
    content.append(Paragraph("ANTÉCÉDENTS FAMILIAUX & MODE DE VIE", styles['SectionHeader']))
    lifestyle_lines = [
        "Non fumeur, alcool: occasionnel.",
        "Ancien fumeur, sevré depuis plus de 10 ans.",
        "Jamais fumeur, pas de consommation d'alcool.",
    ]
    family_lines = [
        "Antécédents familiaux: HTA chez le père, diabète chez la mère.",
        "Antécédents familiaux: sans particularité notable.",
        "Antécédents familiaux: cardiopathie ischémique chez un frère."
    ]
    content.append(Paragraph(fake.random_element(lifestyle_lines), styles['MedicalData']))
    content.append(Paragraph(fake.random_element(family_lines), styles['MedicalData']))
    content.append(Spacer(1, 0.15*inch))

    # Generate clinical notes in French
    clinical_notes = generate_clinical_notes_french(rel, name, age, gender)
    
    # Doctor's Notes Section
    content.append(Paragraph("ÉVALUATION CLINIQUE & NOTES", styles['SectionHeader']))
    content.append(Paragraph(clinical_notes, styles['DoctorNotes']))
    content.append(Spacer(1, 0.15*inch))
    
    # Get provider information
    provider_name = physician_name
    provider_specialty = None
    if "encounters" in rel and not rel["encounters"].empty:
        first_enc = rel["encounters"].iloc[0]
        provider_id = first_enc.get('PROVIDER', '')
        if provider_id and not tables["providers"].empty:
            prov_row = tables["providers"][tables["providers"]["id"] == provider_id]
            if not prov_row.empty:
                prov_name = prov_row.iloc[0].get('NAME', '')
                if prov_name:
                    if not prov_name.startswith('Dr.') and not prov_name.startswith('Dr '):
                        provider_name = f"Dr. {prov_name}"
                    else:
                        provider_name = prov_name
                    provider_specialty = prov_row.iloc[0].get('SPECIALTY', None)
    
    # Physician signature section
    content.append(Spacer(1, 0.2*inch))
    now = datetime.now()
    signature_data = [
        ['Médecin traitant:', provider_name],
    ]
    if provider_specialty:
        signature_data.append(['Spécialité:', provider_specialty])
    signature_data.extend([
        ['Numéro de licence:', fake.numerify(text='MD-######')],
        ['Date:', f"{now.day} {FRENCH_MONTHS[now.month]} {now.year}"],
        ['Signature:', '_________________________']
    ])
    signature_table = Table(signature_data, colWidths=[2*inch, 5*inch])
    signature_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -2), 'Helvetica'),
        ('FONTNAME', (1, 3), (1, 3), 'Helvetica-Oblique'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('LINEABOVE', (0, 0), (-1, 0), 1, colors.HexColor('#cccccc')),
    ]))
    content.append(signature_table)
    
    # Footer
    content.append(Spacer(1, 0.3*inch))
    footer_line = Paragraph("_" * 90, styles['Footer'])
    content.append(footer_line)
    disclaimer = Paragraph(
        "Ce document contient des informations médicales confidentielles. La divulgation non autorisée est interdite par la loi. "
        "Il s'agit d'un dossier médical synthétique généré à des fins de recherche et d'éducation.",
        styles['Footer']
    )
    content.append(disclaimer)

    # Create individual PDF for this patient (limit to max_pdfs)
    if pdf_count < max_pdfs:
        pdf_count += 1
        pdf_filename = pdf_dir / f"patient_{pdf_count}_{pid[:8]}.pdf"
        pdf = SimpleDocTemplate(
            str(pdf_filename),
            pagesize=A4,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        pdf.build(content, onFirstPage=add_page_number, onLaterPages=add_page_number)
    
    # Generate TXT narrative in French
    narrative_txt = f"""
RÉSUMÉ NARRATIF DU PATIENT
----------------------------
Nom: {name}
ID Patient: {pid}
Sexe: {'Masculin' if gender == 'M' else 'Féminin'}, Né(e) le: {format_date_french(birth)}
Âge: {age} ans
Adresse: {address}

Médecin: {physician_name} | Établissement: {org_name if org_name else 'Inconnu'} | Généré par le Système de Données Synthétiques Marocain
"""

    patient_metadata = {
        "id": str(pid),
        "first_name": p.get('FIRST', ''),
        "last_name": p.get('LAST', ''),
        "full_name": name,
        "birthdate": p.get('BIRTHDATE', ''),
        "birthdate_iso": p.get('BIRTHDATE', ''),
        "gender": gender,
        "address": address,
        "city": p.get('CITY', ''),
        "state": p.get('STATE', ''),
        "zip": p.get('ZIP', ''),
        "race": p.get('RACE', ''),
        "ethnicity": p.get('ETHNICITY', ''),
        "ssn": p.get('SSN', ''),
        "primary_organization": org_name if org_name else "Inconnu"
    }
    narrative_txt += "\nMETADONNEES_PATIENT_JSON:\n"
    narrative_txt += json.dumps(patient_metadata, ensure_ascii=False, indent=2)
    narrative_txt += "\n"

    # Add provider specialty if available
    if provider_specialty:
        narrative_txt += f"\nSpécialité du prestataire: {provider_specialty}\n"

    narrative_txt += "\nANTÉCÉDENTS FAMILIAUX & MODE DE VIE:\n"
    narrative_txt += f"- {lifestyle_text}\n"
    narrative_txt += f"- {family_text}\n"

    if conditions_entries:
        narrative_txt += "\nCONDITIONS DIAGNOSTIQUÉES:\n"
        for desc, date_txt in conditions_entries[:5]:
            narrative_txt += f"- {desc} (Diagnostiquée le: {date_txt})\n"
    else:
        narrative_txt += "\nCONDITIONS DIAGNOSTIQUÉES:\n- Aucune condition documentée\n"

    if medications_entries:
        narrative_txt += "\nMÉDICAMENTS:\n"
        for desc, start_txt, stop_txt, status in medications_entries[:5]:
            narrative_txt += f"- {desc} [Du {start_txt} au {stop_txt}] ({status})\n"
    else:
        narrative_txt += "\nMÉDICAMENTS:\n- Aucun traitement actif\n"

    if observations_entries:
        narrative_txt += "\nOBSERVATIONS CLÉS:\n"
        for desc, val, units, date_txt in observations_entries[:6]:
            unit_text = f" {units}" if units and units != 'N/A' else ""
            narrative_txt += f"- {desc}: {val}{unit_text} (Date: {date_txt})\n"
    else:
        narrative_txt += "\nOBSERVATIONS CLÉS:\n- Aucune observation récente\n"

    if procedures_entries:
        narrative_txt += "\nPROCÉDURES:\n"
        for desc, date_txt in procedures_entries[:5]:
            narrative_txt += f"- {desc} (Effectuée le: {date_txt})\n"
    else:
        narrative_txt += "\nPROCÉDURES:\n- Aucune procédure récente\n"

    if immunizations_entries:
        narrative_txt += "\nVACCINATIONS:\n"
        for desc, date_txt in immunizations_entries[:4]:
            narrative_txt += f"- {desc} (Administré le: {date_txt})\n"
    else:
        narrative_txt += "\nVACCINATIONS:\n- Aucune vaccination enregistrée\n"

    if imaging_entries:
        narrative_txt += "\nÉTUDES D'IMAGERIE:\n"
        for date_txt, modality, bodysite in imaging_entries[:3]:
            narrative_txt += f"- Imagerie du {date_txt}, modalité: {modality}, site: {bodysite}\n"
    else:
        narrative_txt += "\nÉTUDES D'IMAGERIE:\n- Aucune étude documentée\n"

    if allergies_entries:
        narrative_txt += "\nALLERGIES:\n"
        for desc, date_txt in allergies_entries[:3]:
            narrative_txt += f"- {desc} (Identifiée le: {date_txt})\n"
    else:
        narrative_txt += "\nALLERGIES:\n- Aucune allergie connue\n"

    if encounters_entries:
        narrative_txt += "\nRÉSUMÉ DES CONSULTATIONS:\n"
        for date_txt, org, enc_class, amount_txt, code in encounters_entries[:3]:
            narrative_txt += f"- Visite le {date_txt} à {org} ({enc_class}, Code: {code}) - Montant: {amount_txt}\n"
        narrative_txt += f"\nFACTURATION ESTIMÉE:\n- Total estimé: {format_currency_mad(total_cost_mad)} (consultation + actes)\n"
    else:
        narrative_txt += "\nRÉSUMÉ DES CONSULTATIONS:\n- Aucune consultation récente\n"

    narrative_txt += f"\nNOTES DU MÉDECIN:\n{clinical_notes}\n\n----------------------------\n\n"
    
    # Save TXT file
    txt_filename = txt_dir / f"patient_{patient_count}_{pid[:8]}.txt"
    txt_filename.write_text(narrative_txt, encoding="utf-8")

print(f"\n✓ Traitement terminé!")
print(f"✓ {pdf_count} PDF générés dans: {pdf_dir}")
print(f"✓ {patient_count} fichiers TXT générés dans: {txt_dir}")
print(f"\nTous les documents sont en français et sans duplication.")