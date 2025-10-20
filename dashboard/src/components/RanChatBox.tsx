import { useState } from 'react';
import { Send, MessageSquare, Trash2, ChevronUp, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import ChatMessage from './ChatMessage';
import { useChatAPI } from '@/hooks/useChatAPI';

const RanChatBox = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [inputValue, setInputValue] = useState('');
  const { messages, isLoading, sendMessage, clearHistory } = useChatAPI();

  const handleSend = async () => {
    if (!inputValue.trim() || isLoading) return;
    
    await sendMessage(inputValue);
    setInputValue('');
  };

  const sampleQueries = [
    "What will be the predicted usage for Cell 100 on August 4th, 2025?",
    "Show me the performance metrics for towers in downtown area",
    "Which cells are expected to have high traffic this week?",
  ];

  const handleSampleQuery = (query: string) => {
    setInputValue(query);
  };

  return (
    <div className="w-full">
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <CollapsibleTrigger asChild>
          <Button
            variant="outline"
            className="w-full glass-glow border-primary/20 hover:bg-primary/10 mb-4"
          >
            <MessageSquare className="w-4 h-4 mr-2" />
            RAN AI Chat
            {isOpen ? <ChevronDown className="w-4 h-4 ml-2" /> : <ChevronUp className="w-4 h-4 ml-2" />}
          </Button>
        </CollapsibleTrigger>
        
        <CollapsibleContent>
          <div className="glass rounded-lg border border-border/20 overflow-hidden">
            {/* Chat Messages */}
            <div className="h-80">
              <ScrollArea className="h-full p-4">
                {messages.length === 0 ? (
                  <div className="text-center text-muted-foreground py-8">
                    <MessageSquare className="w-12 h-12 mx-auto mb-4 opacity-50" />
                    <p className="text-sm mb-4">Ask RAN AI about cell tower predictions and network insights</p>
                    <div className="space-y-2">
                      {sampleQueries.map((query, index) => (
                        <Button
                          key={index}
                          variant="ghost"
                          size="sm"
                          className="text-xs h-auto p-2 text-left block w-full"
                          onClick={() => handleSampleQuery(query)}
                        >
                          "{query}"
                        </Button>
                      ))}
                    </div>
                  </div>
                ) : (
                  <>
                    {messages.map((message) => (
                      <ChatMessage
                        key={message.id}
                        type={message.type}
                        content={message.content}
                        timestamp={message.timestamp}
                      />
                    ))}
                    {isLoading && (
                      <div className="flex gap-3 justify-start mb-4">
                        <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
                          <MessageSquare className="w-4 h-4 text-primary animate-pulse" />
                        </div>
                        <div className="glass bg-card/50 rounded-lg p-3">
                          <div className="flex gap-1">
                            <div className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce"></div>
                            <div className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                            <div className="w-2 h-2 bg-muted-foreground rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                          </div>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </ScrollArea>
            </div>

            {/* Input Area */}
            <div className="border-t border-border/20 p-4">
              <div className="flex gap-2 mb-2">
                <div className="flex-1">
                  <Textarea
                    placeholder="Ask about cell tower predictions, usage patterns, or network insights..."
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault();
                        handleSend();
                      }
                    }}
                    className="min-h-[60px] resize-none"
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Button
                    onClick={handleSend}
                    disabled={!inputValue.trim() || isLoading}
                    size="icon"
                    className="h-[60px] w-12"
                  >
                    <Send className="w-4 h-4" />
                  </Button>
                </div>
              </div>
              
              {messages.length > 0 && (
                <div className="flex justify-end">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={clearHistory}
                    className="text-muted-foreground hover:text-destructive"
                  >
                    <Trash2 className="w-3 h-3 mr-1" />
                    Clear History
                  </Button>
                </div>
              )}
            </div>
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
};

export default RanChatBox;