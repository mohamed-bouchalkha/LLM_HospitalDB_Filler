import axios from 'axios'

// Configuration Groq API
const GROQ_API_KEY = import.meta.env.VITE_GROQ_API_KEY || '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'
const GROQ_API_URL = 'https://api.groq.com/openai/v1/chat/completions'

// Vérifier que la clé API est présente
if (!GROQ_API_KEY || GROQ_API_KEY.trim() === '') {
  console.warn('⚠️ Clé API Groq manquante. Veuillez définir VITE_GROQ_API_KEY dans votre fichier .env')
}

// Configuration de l'API backend personnalisée (optionnel)
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

/**
 * Envoie un message au chatbot via Groq API
 * @param {string} message - Le message de l'utilisateur
 * @param {Array} conversationHistory - Historique de la conversation (optionnel)
 * @returns {Promise<Object>} La réponse du chatbot
 */
export const sendMessage = async (message, conversationHistory = []) => {
  try {
    // Préparer uniquement les messages utilisateur/assistant sans prompt système
    const validHistory = conversationHistory
      .filter(msg => msg && msg.role && msg.content && typeof msg.content === 'string')
      .map(msg => ({
        role: msg.role,
        content: String(msg.content).trim()
      }))
      .filter(msg => msg.content.length > 0)

    const userMessage = {
      role: 'user',
      content: String(message).trim()
    }

    // Construire le tableau de messages sans prompt système
    const messages = [...validHistory, userMessage]

    // Vérifier que le message utilisateur n'est pas vide
    if (!userMessage.content) {
      throw new Error('Le message ne peut pas être vide')
    }

    // Liste des modèles à essayer (du plus rapide au plus puissant)
    const availableModels = [
      'llama-3.1-8b-instant',
      'llama-3.1-70b-versatile',
      'mixtral-8x7b-32768',
      'gemma-7b-it'
    ]

    // Appel à Groq API - juste le message utilisateur
    const requestBody = {
      model: availableModels[0],
      messages: messages,
      temperature: 0.7,
      max_tokens: 1024,
      stream: false
    }

    // Vérifier que la clé API est valide
    if (!GROQ_API_KEY || !GROQ_API_KEY.startsWith('gsk_')) {
      throw new Error('Clé API Groq invalide. La clé doit commencer par "gsk_"')
    }

    const response = await axios.post(
      GROQ_API_URL,
      requestBody,
      {
        headers: {
          'Authorization': `Bearer ${GROQ_API_KEY}`,
          'Content-Type': 'application/json',
        },
        timeout: 30000,
      }
    )

    // Extraire la réponse
    if (!response.data || !response.data.choices || response.data.choices.length === 0) {
      throw new Error('Réponse invalide de l\'API Groq')
    }

    const botResponse = response.data.choices[0]?.message?.content || "Désolé, je n'ai pas pu générer de réponse."
    
    return {
      message: botResponse,
      timestamp: new Date().toISOString()
    }
  } catch (error) {
    console.error('Erreur Groq API:', error)
    
    if (error.response) {
      const status = error.response.status
      const errorData = error.response.data
      
      if (status === 400) {
        const errorMsg = errorData?.error?.message || errorData?.message || 'Requête invalide'
        throw new Error(`Erreur 400: ${errorMsg}`)
      } else if (status === 401) {
        throw new Error('Erreur d\'authentification: Clé API invalide ou manquante.')
      } else if (status === 429) {
        throw new Error('Trop de requêtes. Veuillez patienter avant de réessayer.')
      } else {
        throw new Error(`Erreur ${status}: ${errorData?.error?.message || errorData?.message || 'Erreur inconnue'}`)
      }
    } else if (error.request) {
      throw new Error('Impossible de contacter l\'API Groq. Vérifiez votre connexion internet.')
    } else {
      throw error
    }
  }
}

/**
 * Récupère l'historique des conversations (optionnel)
 * @param {string} patientId - ID du patient
 * @returns {Promise<Array>} L'historique des conversations
 */
export const getChatHistory = async (patientId) => {
  try {
    const response = await apiClient.get(`/chat/history/${patientId}`)
    return response.data
  } catch (error) {
    console.error('Erreur lors de la récupération de l\'historique:', error)
    throw error
  }
}

/**
 * Récupère les événements médicaux d'un patient (optionnel)
 * @param {string} patientId - ID du patient
 * @returns {Promise<Array>} Les événements médicaux
 */
export const getPatientEvents = async (patientId) => {
  try {
    const response = await apiClient.get(`/patients/${patientId}/events`)
    return response.data
  } catch (error) {
    console.error('Erreur lors de la récupération des événements:', error)
    throw error
  }
}

export default apiClient

