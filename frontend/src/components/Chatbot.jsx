import React, { useState, useEffect, useRef } from 'react';
import { Send, Activity, User, Building2, CreditCard, Stethoscope, Database, Loader2, Search, TrendingUp, AlertCircle, CheckCircle, MessageSquare, X } from 'lucide-react';

const MedicalRAGClient = () => {
  const [query, setQuery] = useState('');
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [health, setHealth] = useState(null);
  const [stats, setStats] = useState(null);
  const messagesEndRef = useRef(null);
  const [showWelcome, setShowWelcome] = useState(true);

  const API_URL = 'http://localhost:8000';

  useEffect(() => {
    checkHealth();
    fetchStats();
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const checkHealth = async () => {
    try {
      const response = await fetch(`${API_URL}/health`);
      const data = await response.json();
      setHealth(data);
    } catch (error) {
      console.error('Health check failed:', error);
    }
  };

  const fetchStats = async () => {
    try {
      const response = await fetch(`${API_URL}/stats`);
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Stats fetch failed:', error);
    }
  };

  const handleSubmit = async () => {
    if (!query.trim() || isLoading) return;

    const userMessage = { role: 'user', content: query, timestamp: new Date() };
    setMessages(prev => [...prev, userMessage]);
    setQuery('');
    setIsLoading(true);
    setShowWelcome(false);

    try {
      const response = await fetch(`${API_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, max_results: 100 })
      });

      const data = await response.json();
      
      const botMessage = {
        role: 'assistant',
        content: data.answer,
        timestamp: new Date(),
        metadata: {
          actor_type: data.actor_type,
          actor_id: data.actor_id,
          num_documents: data.num_documents_retrieved,
          num_events: data.num_events,
          documents: data.documents
        }
      };

      setMessages(prev => [...prev, botMessage]);
    } catch (error) {
      const errorMessage = {
        role: 'error',
        content: `Error: ${error.message}`,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const quickQueries = [
    { icon: User, text: 'Tell me about Patient 1', color: 'from-blue-500 to-blue-600' },
    { icon: Stethoscope, text: 'What medications does Patient 5 have?', color: 'from-green-500 to-green-600' },
    { icon: Activity, text: 'Events on October 12, 2023', color: 'from-purple-500 to-purple-600' },
    { icon: Building2, text: 'Tell me about provider 2', color: 'from-orange-500 to-orange-600' }
  ];

  const getActorIcon = (type) => {
    switch(type) {
      case 'patient': return <User className="w-4 h-4" />;
      case 'provider': return <Stethoscope className="w-4 h-4" />;
      case 'organization': return <Building2 className="w-4 h-4" />;
      case 'payer': return <CreditCard className="w-4 h-4" />;
      default: return <Database className="w-4 h-4" />;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50">
      {/* Animated Background */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 left-0 w-96 h-96 bg-blue-200 rounded-full mix-blend-multiply filter blur-3xl opacity-20 animate-pulse"></div>
        <div className="absolute top-0 right-0 w-96 h-96 bg-purple-200 rounded-full mix-blend-multiply filter blur-3xl opacity-20" style={{animationDelay: '1s'}}></div>
        <div className="absolute bottom-0 left-1/2 w-96 h-96 bg-green-200 rounded-full mix-blend-multiply filter blur-3xl opacity-20" style={{animationDelay: '2s'}}></div>
      </div>

      {/* Header */}
      <header className="relative bg-white/80 backdrop-blur-md border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="relative">
                <Activity className="w-8 h-8 text-blue-600" />
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                  Medical Data Warehouse
                </h1>
                <p className="text-xs text-gray-500">AI-Powered Healthcare Analytics</p>
              </div>
            </div>
            
            {health && (
              <div className="flex items-center space-x-4">
                <div className={`flex items-center space-x-2 px-3 py-1.5 rounded-full ${health.database_loaded ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                  {health.database_loaded ? <CheckCircle className="w-4 h-4" /> : <AlertCircle className="w-4 h-4" />}
                  <span className="text-xs font-medium">{health.database_loaded ? 'Online' : 'Offline'}</span>
                </div>
                <div className="px-3 py-1.5 bg-purple-100 text-purple-700 rounded-full text-xs font-medium">
                  {health.device.toUpperCase()}
                </div>
              </div>
            )}
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Stats Cards */}
        {stats && (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            {[
              { label: 'Documents', value: stats.total_documents, icon: Database, color: 'blue' },
              { label: 'Patients', value: stats.unique_patients, icon: User, color: 'green' },
              { label: 'Providers', value: stats.unique_providers, icon: Stethoscope, color: 'purple' },
              { label: 'Organizations', value: stats.unique_organizations, icon: Building2, color: 'orange' },
              { label: 'Events', value: stats.total_events, icon: Activity, color: 'red' }
            ].map((stat, idx) => (
              <div key={idx} className="bg-white rounded-xl p-4 shadow-sm border border-gray-100 hover:shadow-md transition-all duration-300 transform hover:-translate-y-1">
                <div className="flex items-center justify-between mb-2">
                  <stat.icon className={`w-5 h-5 text-${stat.color}-600`} />
                  <TrendingUp className="w-4 h-4 text-gray-400" />
                </div>
                <div className="text-2xl font-bold text-gray-900">{stat.value.toLocaleString()}</div>
                <div className="text-xs text-gray-500">{stat.label}</div>
              </div>
            ))}
          </div>
        )}

        {/* Main Content */}
        <div className="bg-white rounded-2xl shadow-xl border border-gray-200 overflow-hidden">
          {/* Chat Area */}
          <div className="h-[500px] overflow-y-auto p-6 space-y-4">
            {showWelcome && messages.length === 0 && (
              <div className="text-center py-12">
                <div className="mb-6 relative inline-block">
                  <MessageSquare className="w-20 h-20 text-blue-500 mx-auto animate-bounce" />
                  <div className="absolute -top-2 -right-2 w-6 h-6 bg-green-500 rounded-full flex items-center justify-center">
                    <Activity className="w-4 h-4 text-white" />
                  </div>
                </div>
                <h2 className="text-2xl font-bold text-gray-900 mb-2">Welcome to Medical RAG</h2>
                <p className="text-gray-600 mb-8">Ask me anything about patients, providers, organizations, or medical events</p>
                
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 max-w-2xl mx-auto">
                  {quickQueries.map((q, idx) => (
                    <button
                      key={idx}
                      onClick={() => setQuery(q.text)}
                      className={`flex items-center space-x-3 p-4 bg-gradient-to-r ${q.color} text-white rounded-lg hover:shadow-lg transition-all duration-300 transform hover:scale-105`}
                    >
                      <q.icon className="w-5 h-5" />
                      <span className="text-sm font-medium">{q.text}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, idx) => (
              <div
                key={idx}
                className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                style={{animation: 'slideIn 0.3s ease-out'}}
              >
                <div className={`max-w-3xl`}>
                  {msg.role === 'assistant' && msg.metadata && (
                    <div className="flex items-center space-x-2 mb-2 ml-2">
                      {msg.metadata.actor_type && (
                        <div className="flex items-center space-x-1 px-2 py-1 bg-blue-100 text-blue-700 rounded-full text-xs">
                          {getActorIcon(msg.metadata.actor_type)}
                          <span className="capitalize">{msg.metadata.actor_type}</span>
                          {msg.metadata.actor_id && <span>#{msg.metadata.actor_id}</span>}
                        </div>
                      )}
                      <div className="px-2 py-1 bg-green-100 text-green-700 rounded-full text-xs">
                        {msg.metadata.num_events} events
                      </div>
                      <div className="px-2 py-1 bg-purple-100 text-purple-700 rounded-full text-xs">
                        {msg.metadata.num_documents} docs
                      </div>
                    </div>
                  )}
                  
                  <div className={`rounded-2xl p-4 ${
                    msg.role === 'user' 
                      ? 'bg-gradient-to-r from-blue-600 to-blue-700 text-white' 
                      : msg.role === 'error'
                      ? 'bg-red-50 text-red-700 border border-red-200'
                      : 'bg-gray-50 text-gray-900 border border-gray-200'
                  }`}>
                    <div className="whitespace-pre-wrap break-words">{msg.content}</div>
                    <div className={`text-xs mt-2 ${msg.role === 'user' ? 'text-blue-200' : 'text-gray-400'}`}>
                      {new Date(msg.timestamp).toLocaleTimeString()}
                    </div>
                  </div>
                </div>
              </div>
            ))}

            {isLoading && (
              <div className="flex justify-start animate-pulse">
                <div className="bg-gray-50 border border-gray-200 rounded-2xl p-4 flex items-center space-x-3">
                  <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
                  <span className="text-gray-600">Analyzing medical data...</span>
                </div>
              </div>
            )}
            
            <div ref={messagesEndRef} />
          </div>

          {/* Input Area */}
          <div className="border-t border-gray-200 p-4 bg-gray-50">
            <div className="flex space-x-3">
              <div className="flex-1 relative">
                <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyPress={handleKeyPress}
                  placeholder="Ask about patients, providers, medications, events..."
                  className="w-full pl-12 pr-4 py-3 border border-gray-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200"
                  disabled={isLoading}
                />
              </div>
              <button
                onClick={handleSubmit}
                disabled={isLoading || !query.trim()}
                className="px-6 py-3 bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-xl hover:from-blue-700 hover:to-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200 flex items-center space-x-2 shadow-lg hover:shadow-xl transform hover:scale-105"
              >
                {isLoading ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Send className="w-5 h-5" />
                )}
                <span className="font-medium">Send</span>
              </button>
            </div>
            
            <div className="mt-3 flex items-center justify-between text-xs text-gray-500">
              <span>💡 Try: "Patient 1 medications" or "Provider 2 patients in October 2023"</span>
              {messages.length > 0 && (
                <button
                  onClick={() => {setMessages([]); setShowWelcome(true);}}
                  className="flex items-center space-x-1 text-red-600 hover:text-red-700"
                >
                  <X className="w-3 h-3" />
                  <span>Clear</span>
                </button>
              )}
            </div>
          </div>
        </div>
      </div>

      <style>{`
        @keyframes slideIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      `}</style>
    </div>
  );
};

export default MedicalRAGClient;