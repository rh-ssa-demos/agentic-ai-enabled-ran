import { useState } from 'react';
import { useToast } from '@/hooks/use-toast';

interface ChatMessage {
  id: string;
  type: 'user' | 'ai';
  content: string;
  timestamp: Date;
}

interface UseChatAPIReturn {
  messages: ChatMessage[];
  isLoading: boolean;
  sendMessage: (query: string) => Promise<void>;
  clearHistory: () => void;
}

export const useChatAPI = (): UseChatAPIReturn => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const { toast } = useToast();

  const sendMessage = async (query: string) => {
    if (!query.trim()) return;

    // Add user message
    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      type: 'user',
      content: query,
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setIsLoading(true);

    try {
      console.log('Attempting to send message:', query);
      
      // Use proxy endpoint to avoid CORS issues
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      console.log('Response status:', response.status);
      console.log('Response headers:', response.headers);

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const contentType = response.headers.get('content-type');
      console.log('Content-Type:', contentType);
      
      let data;
      if (contentType && contentType.includes('application/json')) {
        const jsonData = await response.json();
        data = jsonData.response || jsonData.answer || JSON.stringify(jsonData);
      } else {
        data = await response.text();
      }
      
      console.log('API Response:', data);

      // Check if we got HTML instead of API response
      if (data.includes('<!DOCTYPE html>')) {
        throw new Error('Received HTML instead of API response - proxy not working');
      }
      
      // Add AI response
      const aiMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        type: 'ai',
        content: data,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, aiMessage]);
    } catch (error) {
      console.error('Chat API error:', error);
      
      // Try fallback with different approach
      try {
        console.log('Trying fallback approach...');
        const fallbackResponse = await fetch('/api/chat', {
          method: 'POST',
          headers: {
            'Content-Type': 'text/plain',
          },
          body: query, // Send as plain text
        });

        if (fallbackResponse.ok) {
          const fallbackData = await fallbackResponse.text();
          
          if (!fallbackData.includes('<!DOCTYPE html>')) {
            const aiMessage: ChatMessage = {
              id: (Date.now() + 1).toString(),
              type: 'ai',
              content: fallbackData,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, aiMessage]);
            return;
          }
        }
      } catch (fallbackError) {
        console.error('Fallback also failed:', fallbackError);
      }

      // Show detailed error message
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      toast({
        title: "Connection Error",
        description: `Failed to connect to RAN AI: ${errorMessage}. Check browser console for details.`,
        variant: "destructive",
      });
      
      // Add error message to chat
      const errorChatMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        type: 'ai',
        content: `⚠️ Connection Error: Unable to reach RAN AI service. This might be due to:\n\n1. CORS restrictions from the API server\n2. Network connectivity issues\n3. API server being unavailable\n\nError details: ${errorMessage}`,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorChatMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const clearHistory = () => {
    setMessages([]);
  };

  return {
    messages,
    isLoading,
    sendMessage,
    clearHistory,
  };
};