# Chatbot Médical RAG

Interface React avec Tailwind CSS pour un chatbot RAG (Retrieval-Augmented Generation) destiné aux questions des patients concernant leurs rendez-vous et événements médicaux.

## 🚀 Installation

1. Installer les dépendances :
```bash
npm install
```

2. Configurer l'API :
   - Créer un fichier `.env` à la racine du projet
   - Ajouter : `VITE_API_BASE_URL=http://localhost:8000/api`
   - Modifier l'URL selon votre configuration backend

3. Lancer le serveur de développement :
```bash
npm run dev
```

## 📁 Structure du projet

```
front/
├── src/
│   ├── components/
│   │   └── Chatbot.jsx      # Composant principal du chatbot
│   ├── services/
│   │   └── api.js           # Service API pour communiquer avec le backend
│   ├── App.jsx              # Composant principal de l'application
│   ├── main.jsx             # Point d'entrée
│   └── index.css            # Styles Tailwind
├── index.html
├── package.json
├── vite.config.js
├── tailwind.config.js
└── postcss.config.js
```

## 🔌 Intégration API

Le service API (`src/services/api.js`) est configuré pour communiquer avec votre backend. 

### Endpoint attendu :

**POST** `/api/chat`
```json
{
  "message": "Quand est mon prochain rendez-vous ?"
}
```

**Réponse attendue :**
```json
{
  "message": "Votre prochain rendez-vous est le...",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

### Personnalisation

Vous pouvez modifier :
- L'URL de base dans `.env` (variable `VITE_API_BASE_URL`)
- Le format des requêtes/réponses dans `src/services/api.js`
- L'interface dans `src/components/Chatbot.jsx`

## 🎨 Fonctionnalités

- ✅ Interface moderne et responsive avec Tailwind CSS
- ✅ Chat en temps réel avec historique des messages
- ✅ Indicateur de chargement pendant les requêtes
- ✅ Gestion des erreurs
- ✅ Prêt pour l'intégration avec votre API RAG
- ✅ Design adapté au contexte médical

## 🛠️ Technologies

- React 18
- Vite
- Tailwind CSS
- Axios

## 📝 Notes

- L'interface est prête à être connectée à votre API backend
- Modifiez `src/services/api.js` pour adapter le format des requêtes selon votre API
- Le design est optimisé pour les questions médicales et les événements patients

