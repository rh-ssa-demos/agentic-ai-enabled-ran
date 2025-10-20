import React, { useState } from 'react';
import { Event } from '../types/network';
import { Bell, BellOff, Phone, TowerControl, ArrowUp, ArrowDown, Database, Settings, CheckCircle, MapPin, Signal, AlertTriangle } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { toZonedTime } from 'date-fns-tz';
import { dbConfig } from '../config/dbConfig';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import EventDetailsDialog from './EventDetailsDialog';
interface LiveFeedProps {
  events: Event[];
}
const LiveFeed: React.FC<LiveFeedProps> = ({
  events
}) => {
  const [selectedCell, setSelectedCell] = useState<string>('all');
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  // Get unique cell IDs from events for filter options
  const uniqueCellIds = Array.from(new Set(events.map(event => event.cellId).filter(Boolean))).sort();

  // Filter events based on selected cell
  const filteredEvents = selectedCell === 'all' ? events : events.filter(event => event.cellId === selectedCell);
  const getEventIcon = (eventType: Event['type']) => {
    switch (eventType) {
      case 'call-placed':
        return <Phone size={20} className="text-blue-400" />;
      case 'call-dropped':
        return <Phone size={20} className="text-yellow-400" />;
      case 'tower-up':
        return <ArrowUp size={20} className="text-green-400" />;
      case 'tower-down':
        return <ArrowDown size={20} className="text-red-400" />;
      case 'alert-triggered':
        return <Bell size={20} className="text-amber-400" />;
      case 'alert-resolved':
        return <BellOff size={20} className="text-blue-400" />;
      case 'remediation-started':
        return <Settings size={20} className="text-orange-400" />;
      case 'remediation-completed':
        return <CheckCircle size={20} className="text-green-400" />;
      case 'anomaly-detected':
        return <AlertTriangle size={20} className="text-red-400" />;
      default:
        return <TowerControl size={20} className="text-gray-400" />;
    }
  };
  const getEventClassNames = (eventType: Event['type']) => {
    const baseClasses = "flex items-start space-x-3 p-3 rounded-md mb-2 transition-all duration-300 glass hover:glass-hover backdrop-blur-sm cursor-pointer hover:scale-[1.02] active:scale-[0.98]";
    switch (eventType) {
      case 'tower-down':
        return `${baseClasses} bg-red-500/10 border border-red-400/30 hover:bg-red-500/20`;
      case 'tower-up':
        return `${baseClasses} bg-green-500/10 border border-green-400/30 hover:bg-green-500/20`;
      case 'call-dropped':
        return `${baseClasses} bg-yellow-500/10 border border-yellow-400/30 hover:bg-yellow-500/20`;
      case 'call-placed':
        return `${baseClasses} bg-blue-500/10 border border-blue-400/30 hover:bg-blue-500/20`;
      case 'alert-triggered':
        return `${baseClasses} bg-amber-500/10 border border-amber-400/30 hover:bg-amber-500/20`;
      case 'remediation-started':
        return `${baseClasses} bg-orange-500/10 border border-orange-400/30 hover:bg-orange-500/20`;
      case 'remediation-completed':
        return `${baseClasses} bg-green-500/10 border border-green-400/30 hover:bg-green-500/20`;
      case 'anomaly-detected':
        return `${baseClasses} bg-red-500/10 border border-red-400/30 hover:bg-red-500/20`;
      default:
        return `${baseClasses} bg-white/5 border border-white/20 hover:bg-white/10`;
    }
  };
  const getEventTitle = (event: Event) => {
    switch (event.type) {
      case 'call-placed':
        return `Call Placed`;
      case 'call-dropped':
        return `Call Dropped`;
      case 'tower-up':
        return `Tower Online`;
      case 'tower-down':
        return `Tower Offline`;
      case 'alert-triggered':
        return `Alert Triggered`;
      case 'alert-resolved':
        return `Alert Resolved`;
      case 'remediation-started':
        return `Remediation Started`;
      case 'remediation-completed':
        return `Remediation Completed`;
      case 'anomaly-detected':
        return `Anomaly Detected`;
      default:
        return `Unknown Event`;
    }
  };

  // Determine data source display text
  const getDataSourceText = () => {
    if (dbConfig.useMock) {
      return 'Mock DB';
    } else {
      return 'JSON';
    }
  };

  // Format date properly, ensuring we're using the actual timestamp
  const formatEventTime = (date: Date) => {
    // Make sure we're working with a valid Date object
    const eventDate = new Date(date);

    // Validate the date isn't invalid or in the future
    const now = new Date();
    if (isNaN(eventDate.getTime())) {
      return 'Invalid date';
    }
    if (eventDate > now) {
      return 'Future event';
    }

    // Convert to local timezone for display
    const localDate = toZonedTime(eventDate, Intl.DateTimeFormat().resolvedOptions().timeZone);

    // Use formatDistanceToNow which handles the relative time conversion
    return formatDistanceToNow(localDate, {
      addSuffix: true
    });
  };
  const handleEventClick = (event: Event) => {
    setSelectedEvent(event);
    setIsDialogOpen(true);
  };
  const closeDialog = () => {
    setIsDialogOpen(false);
    setSelectedEvent(null);
  };
  const handleKeyDown = (event: React.KeyboardEvent, networkEvent: Event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      handleEventClick(networkEvent);
    }
  };
  return <div className="glass-card p-4 glass-glow">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-bold text-foreground drop-shadow-lg">Live Events</h2>
        <div className="flex items-center gap-2">
          <Select value={selectedCell} onValueChange={setSelectedCell}>
            <SelectTrigger className="w-40 glass border-border text-foreground bg-card/50 backdrop-blur-md hover:bg-card/80 transition-all">
              <SelectValue placeholder="All Cells" />
            </SelectTrigger>
            <SelectContent className="bg-card/95 border-border backdrop-blur-lg text-foreground">
              <SelectItem value="all" className="text-foreground hover:bg-muted focus:bg-muted focus:text-foreground">All Cells</SelectItem>
              {uniqueCellIds.map(cellId => <SelectItem key={cellId} value={cellId} className="text-foreground hover:bg-muted focus:bg-muted focus:text-foreground">
                  {cellId}
                </SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      </div>
      
      <div className="max-h-[400px] overflow-y-auto pr-2 glass-scrollbar">
        {filteredEvents.length === 0 ? <div className="text-center py-8 text-muted-foreground">
            <TowerControl size={40} className="mx-auto mb-3 text-muted-foreground/60" />
            <p>{selectedCell === 'all' ? 'No events recorded yet' : `No events for ${selectedCell}`}</p>
          </div> : filteredEvents.map((event, index) => <div key={index} className={getEventClassNames(event.type)} onClick={() => handleEventClick(event)} onKeyDown={e => handleKeyDown(e, event)} tabIndex={0} role="button" aria-label={`View details for ${getEventTitle(event)}`}>
              <div className="p-2 glass rounded-md backdrop-blur-sm">
                {getEventIcon(event.type)}
              </div>
              <div className="flex-1">
                <div className="flex justify-between">
                  <h3 className="font-medium text-foreground drop-shadow-sm">{getEventTitle(event)}</h3>
                  <span className="text-xs text-muted-foreground">
                    {formatEventTime(event.timestamp)}
                  </span>
                </div>
          {event.message && event.type !== 'anomaly-detected' && <p className="text-sm text-foreground/80 mt-1">{event.message}</p>}
                {(event.type === 'call-placed' || event.type === 'call-dropped') && event.location && event.signalStrength && <div className="space-y-1 mt-2">
                    <div className="flex items-center gap-2 text-sm text-foreground/80">
                      <MapPin size={14} className="text-blue-400 drop-shadow-sm" />
                      <span>Location: {event.location.lat.toFixed(4)}, {event.location.lng.toFixed(4)}</span>
                    </div>
                    <div className="flex items-center gap-2 text-sm text-foreground/80">
                      <Signal size={14} className="text-green-400 drop-shadow-sm" />
                      <span>Signal: {event.signalStrength} dBm</span>
                    </div>
                  </div>}
                 {event.type === 'anomaly-detected' && (event.anomalyType || event.band) && <div className="space-y-1 mt-2">
                     {event.anomalyType && <div className="flex items-center gap-2 text-sm text-foreground/80">
                         <AlertTriangle size={14} className="text-red-400 drop-shadow-sm" />
                         <span>Type: {event.anomalyType}</span>
                       </div>}
                     {event.band && <div className="flex items-center gap-2 text-sm text-foreground/80">
                         <Signal size={14} className="text-orange-400 drop-shadow-sm" />
                         <span>Band: {event.band}</span>
                       </div>}
                   </div>}
                 <div className="flex flex-wrap gap-2 mt-2">
                  {event.cellId && <div className="text-xs glass-dark px-2 py-1 rounded-sm text-foreground/80 backdrop-blur-sm">
                      Cell: {event.cellId}
                    </div>}
                </div>
              </div>
            </div>)}
      </div>
      
      <EventDetailsDialog event={selectedEvent} isOpen={isDialogOpen} onClose={closeDialog} />
    </div>;
};
export default LiveFeed;