import React, { useState, useRef, useEffect } from 'react'
import { sendMessage } from '../services/api'

// Message de bienvenue par défaut
const defaultWelcomeMessage = {
  id: 1,
  text: "Bonjour ! Je suis votre assistant médical. Comment puis-je vous aider aujourd'hui concernant vos rendez-vous ou événements médicaux ?",
  sender: 'bot',
  timestamp: new Date()
}

const Chatbot = () => {
  const [messages, setMessages] = useState([defaultWelcomeMessage])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [showScrollTop, setShowScrollTop] = useState(false)
  const messagesEndRef = useRef(null)
  const messagesContainerRef = useRef(null)

  const scrollToBottom = (force = false) => {
    if (messagesContainerRef.current) {
      const container = messagesContainerRef.current
      
      if (force) {
        // Forcer le scroll vers le bas
        setTimeout(() => {
          if (messagesEndRef.current) {
            messagesEndRef.current.scrollIntoView({ behavior: 'smooth', block: 'end' })
          } else {
            container.scrollTop = container.scrollHeight
          }
        }, 100)
      } else {
        // Ne scroll que si on est déjà proche du bas
        const isNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 150
        if (isNearBottom) {
          setTimeout(() => {
            if (messagesEndRef.current) {
              messagesEndRef.current.scrollIntoView({ behavior: 'smooth', block: 'end' })
            } else {
              container.scrollTop = container.scrollHeight
            }
          }, 100)
        }
      }
    }
  }

  useEffect(() => {
    // Toujours scroller vers le bas quand un nouveau message arrive
    scrollToBottom(true)
  }, [messages.length])

  // Détecter le scroll pour afficher/masquer le bouton "remonter"
  useEffect(() => {
    const container = messagesContainerRef.current
    if (!container) return

    const handleScroll = () => {
      // Afficher le bouton si on n'est pas tout en haut
      const isScrolled = container.scrollTop > 100
      setShowScrollTop(isScrolled)
    }

    container.addEventListener('scroll', handleScroll)
    return () => container.removeEventListener('scroll', handleScroll)
  }, [])

  // Fonction pour remonter en haut
  const scrollToTop = () => {
    if (messagesContainerRef.current) {
      messagesContainerRef.current.scrollTo({
        top: 0,
        behavior: 'smooth'
      })
    }
  }

  // Fonction pour effacer l'historique
  const clearHistory = () => {
    if (window.confirm('Êtes-vous sûr de vouloir effacer l\'historique de la conversation ?')) {
      setMessages([defaultWelcomeMessage])
    }
  }

  // Fonction pour formater la date
  const formatDate = (date) => {
    const now = new Date()
    const messageDate = new Date(date)
    const diffTime = Math.abs(now - messageDate)
    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24))

    if (diffDays === 0) {
      return messageDate.toLocaleTimeString('fr-FR', { 
        hour: '2-digit', 
        minute: '2-digit' 
      })
    } else if (diffDays === 1) {
      return `Hier ${messageDate.toLocaleTimeString('fr-FR', { 
        hour: '2-digit', 
        minute: '2-digit' 
      })}`
    } else if (diffDays < 7) {
      return messageDate.toLocaleDateString('fr-FR', { 
        weekday: 'short',
        hour: '2-digit', 
        minute: '2-digit' 
      })
    } else {
      return messageDate.toLocaleDateString('fr-FR', { 
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit', 
        minute: '2-digit' 
      })
    }
  }

  const handleSend = async (e) => {
    e.preventDefault()
    if (!input.trim() || isLoading) return

    const userMessage = {
      id: messages.length + 1,
      text: input.trim(),
      sender: 'user',
      timestamp: new Date()
    }

    setMessages(prev => [...prev, userMessage])
    const currentInput = input.trim()
    setInput('')
    setIsLoading(true)
    
    // Scroller vers le bas après l'ajout du message utilisateur
    setTimeout(() => scrollToBottom(true), 50)

    try {
      // Préparer l'historique de conversation pour Groq
      const conversationHistory = messages
        .filter(msg => msg.id !== 1) // Exclure le message de bienvenue
        .map(msg => ({
          role: msg.sender === 'user' ? 'user' : 'assistant',
          content: msg.text
        }))

      const response = await sendMessage(currentInput, conversationHistory)
      const botMessage = {
        id: messages.length + 2,
        text: response.message || response.text || "Désolé, je n'ai pas pu traiter votre demande.",
        sender: 'bot',
        timestamp: new Date()
      }
      setMessages(prev => [...prev, botMessage])
      // Forcer le scroll vers le bas après la réponse
      setTimeout(() => scrollToBottom(true), 200)
    } catch (error) {
      console.error('Erreur lors de l\'envoi du message:', error)
      let errorText = "Désolé, une erreur s'est produite. Veuillez réessayer plus tard."
      
      // Messages d'erreur plus détaillés
      if (error.response?.status === 400) {
        errorText = error.message || "Erreur 400: Requête invalide. Vérifiez le format de votre message."
      } else if (error.response?.status === 401) {
        errorText = "Erreur d'authentification API. Vérifiez votre clé API."
      } else if (error.response?.status === 429) {
        errorText = "Trop de requêtes. Veuillez patienter un moment avant de réessayer."
      } else if (error.message) {
        errorText = error.message
      }
      
      const errorMessage = {
        id: messages.length + 2,
        text: errorText,
        sender: 'bot',
        timestamp: new Date()
      }
      setMessages(prev => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="bg-white/80 backdrop-blur-lg rounded-2xl shadow-2xl flex flex-col border border-white/20 animate-fade-in relative" style={{ height: '100%', maxHeight: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Header avec effet glassmorphism - Fixe en haut */}
      <div className="bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 text-white p-4 relative overflow-hidden flex-shrink-0 z-50 shadow-lg" style={{ flexShrink: 0 }}>
        <div className="absolute inset-0 bg-gradient-to-r from-blue-600/50 to-purple-600/50"></div>
        <div className="relative flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="relative">
              <div className="w-12 h-12 bg-white/20 backdrop-blur-sm rounded-full flex items-center justify-center border-2 border-white/30 shadow-lg transform hover:scale-110 transition-transform duration-300">
                <svg className="w-7 h-7 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <div className="absolute -top-1 -right-1 w-4 h-4 bg-green-400 rounded-full border-2 border-white animate-pulse"></div>
            </div>
            <div>
              <h2 className="font-bold text-xl">Assistant Médical</h2>
              <p className="text-sm text-blue-100 flex items-center space-x-1">
                <span className="w-2 h-2 bg-green-300 rounded-full animate-pulse"></span>
                <span>En ligne</span>
              </p>
            </div>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={clearHistory}
              className="p-2 bg-white/20 hover:bg-white/30 rounded-lg transition-all duration-200 backdrop-blur-sm border border-white/30 hover:scale-110"
              title="Effacer l'historique"
            >
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
            <div className="flex space-x-2">
              <div className="w-2 h-2 bg-white/60 rounded-full animate-pulse"></div>
              <div className="w-2 h-2 bg-white/60 rounded-full animate-pulse" style={{ animationDelay: '0.2s' }}></div>
              <div className="w-2 h-2 bg-white/60 rounded-full animate-pulse" style={{ animationDelay: '0.4s' }}></div>
            </div>
          </div>
        </div>
      </div>

      {/* Messages - Zone scrollable avec scrollbar visible */}
      <div 
        ref={messagesContainerRef}
        className="p-6 space-y-4 bg-gradient-to-b from-gray-50 to-gray-100 relative"
        style={{ 
          flex: '1 1 0',
          overflowY: 'scroll',
          overflowX: 'hidden',
          minHeight: 0,
          maxHeight: '100%',
          scrollBehavior: 'smooth',
          WebkitOverflowScrolling: 'touch'
        }}
      >
        {messages.map((message, index) => (
          <div
            key={message.id}
            className={`flex ${message.sender === 'user' ? 'justify-end' : 'justify-start'} ${
              message.sender === 'user' ? 'animate-slide-in-right' : 'animate-slide-in-left'
            }`}
            style={{ animationDelay: `${index * 0.1}s` }}
          >
            <div className="flex items-end space-x-2 max-w-[80%]" data-message-id={message.id}>
              {message.sender === 'bot' && (
                <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-full flex items-center justify-center flex-shrink-0 shadow-md">
                  <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                  </svg>
                </div>
              )}
              <div
                className={`rounded-2xl px-5 py-3 shadow-lg transform transition-all duration-300 hover:scale-[1.02] ${
                  message.sender === 'user'
                    ? 'bg-gradient-to-br from-blue-500 to-indigo-600 text-white rounded-br-md'
                    : 'bg-white text-gray-800 rounded-bl-md border border-gray-200/50 backdrop-blur-sm'
                }`}
              >
                <p className="text-sm leading-relaxed whitespace-pre-wrap">{message.text}</p>
                <p className={`text-xs mt-2 ${
                  message.sender === 'user' ? 'text-blue-100' : 'text-gray-400'
                }`}>
                  {formatDate(message.timestamp)}
                </p>
              </div>
              {message.sender === 'user' && (
                <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center flex-shrink-0 shadow-md">
                  <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                  </svg>
                </div>
              )}
            </div>
          </div>
        ))}
        {isLoading && (
          <div className="flex justify-start animate-fade-in">
            <div className="flex items-end space-x-2">
              <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-full flex items-center justify-center flex-shrink-0 shadow-md">
                <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              </div>
              <div className="bg-white text-gray-800 rounded-2xl rounded-bl-md px-5 py-3 shadow-lg border border-gray-200/50">
                <div className="flex space-x-2">
                  <div className="w-2.5 h-2.5 bg-blue-500 rounded-full animate-bounce"></div>
                  <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                  <div className="w-2.5 h-2.5 bg-purple-500 rounded-full animate-bounce" style={{ animationDelay: '0.4s' }}></div>
                </div>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />

        {/* Bouton pour remonter en haut */}
        {showScrollTop && (
          <button
            onClick={scrollToTop}
            className="fixed bottom-24 right-8 bg-gradient-to-r from-blue-500 to-indigo-600 text-white p-3 rounded-full shadow-lg hover:shadow-xl transform hover:scale-110 transition-all duration-300 z-40 animate-fade-in"
            title="Remonter en haut"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 10l7-7m0 0l7 7m-7-7v18" />
            </svg>
          </button>
        )}
      </div>

      {/* Input */}
      <form onSubmit={handleSend} className="border-t border-gray-200/50 p-4 bg-white/80 backdrop-blur-sm flex-shrink-0">
        <div className="flex space-x-3">
          <div className="flex-1 relative">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Posez votre question..."
              className="w-full px-5 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-300 bg-white/90 backdrop-blur-sm shadow-sm hover:shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isLoading}
            />
            <div className="absolute right-3 top-1/2 transform -translate-y-1/2">
              <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
              </svg>
            </div>
          </div>
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="px-6 py-3 bg-gradient-to-r from-blue-500 to-indigo-600 text-white rounded-xl hover:from-blue-600 hover:to-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-300 flex items-center space-x-2 shadow-lg hover:shadow-xl transform hover:scale-105 active:scale-95 font-medium"
          >
            <span>Envoyer</span>
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
            </svg>
          </button>
        </div>
        <div className="mt-3 flex items-center justify-center space-x-4 text-xs text-gray-500">
          <div className="flex items-center space-x-1">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            <span>Rapide</span>
          </div>
          <div className="flex items-center space-x-1">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
            </svg>
            <span>Sécurisé</span>
          </div>
          <div className="flex items-center space-x-1">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
            </svg>
            <span>Intelligent</span>
          </div>
        </div>
      </form>

    </div>
  )
}

export default Chatbot

