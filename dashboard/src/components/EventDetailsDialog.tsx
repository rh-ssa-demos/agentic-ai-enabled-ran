import React from 'react';
import { Event } from '../types/network';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from './ui/dialog';
import { 
  Bell, 
  BellOff, 
  Phone, 
  TowerControl, 
  ArrowUp, 
  ArrowDown, 
  Settings, 
  CheckCircle, 
  MapPin, 
  Signal, 
  AlertTriangle,
  Calendar,
  Smartphone,
  Radio,
  Database,
  Wrench
} from 'lucide-react';
import { formatDistanceToNow, format } from 'date-fns';
import { Badge } from './ui/badge';

interface EventDetailsDialogProps {
  event: Event | null;
  isOpen: boolean;
  onClose: () => void;
}

const EventDetailsDialog: React.FC<EventDetailsDialogProps> = ({
  event,
  isOpen,
  onClose,
}) => {
  if (!event) return null;

  const getEventIcon = (eventType: Event['type']) => {
    switch (eventType) {
      case 'call-placed':
        return <Phone className="text-blue-400" />;
      case 'call-dropped':
        return <Phone className="text-yellow-400" />;
      case 'tower-up':
        return <ArrowUp className="text-green-400" />;
      case 'tower-down':
        return <ArrowDown className="text-red-400" />;
      case 'alert-triggered':
        return <Bell className="text-amber-400" />;
      case 'alert-resolved':
        return <BellOff className="text-blue-400" />;
      case 'remediation-started':
        return <Settings className="text-orange-400" />;
      case 'remediation-completed':
        return <CheckCircle className="text-green-400" />;
      case 'anomaly-detected':
        return <AlertTriangle className="text-red-400" />;
      default:
        return <TowerControl className="text-gray-400" />;
    }
  };

  const getEventTitle = (event: Event) => {
    switch (event.type) {
      case 'call-placed':
        return 'Call Placed';
      case 'call-dropped':
        return 'Call Dropped';
      case 'tower-up':
        return 'Tower Online';
      case 'tower-down':
        return 'Tower Offline';
      case 'alert-triggered':
        return 'Alert Triggered';
      case 'alert-resolved':
        return 'Alert Resolved';
      case 'remediation-started':
        return 'Remediation Started';
      case 'remediation-completed':
        return 'Remediation Completed';
      case 'anomaly-detected':
        return 'Anomaly Detected';
      default:
        return 'Unknown Event';
    }
  };

  const getEventBadgeVariant = (eventType: Event['type']) => {
    switch (eventType) {
      case 'tower-down':
      case 'call-dropped':
      case 'anomaly-detected':
      case 'alert-triggered':
        return 'destructive';
      case 'tower-up':
      case 'remediation-completed':
      case 'alert-resolved':
        return 'default';
      case 'call-placed':
      case 'remediation-started':
        return 'secondary';
      default:
        return 'outline';
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto glass-card border-border text-foreground bg-card">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-3 text-xl text-foreground">
            <div className="p-2 glass rounded-md backdrop-blur-sm">
              {getEventIcon(event.type)}
            </div>
            {getEventTitle(event)}
          </DialogTitle>
          <DialogDescription className="text-muted-foreground">
            Detailed information about this network event
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 mt-4">
          {/* Basic Information */}
          <div className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Calendar size={16} />
                  <span>Timestamp</span>
                </div>
                <div className="glass p-3 rounded-md">
                  <p className="text-foreground font-medium">
                    {format(new Date(event.timestamp), 'PPpp')}
                  </p>
                  <p className="text-muted-foreground text-sm">
                    {formatDistanceToNow(new Date(event.timestamp), { addSuffix: true })}
                  </p>
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Database size={16} />
                  <span>Event Type</span>
                </div>
                <div className="glass p-3 rounded-md">
                  <Badge variant={getEventBadgeVariant(event.type)} className="text-sm">
                    {event.type.replace('-', ' ').toUpperCase()}
                  </Badge>
                </div>
              </div>
            </div>

            {/* Cell Information */}
            {event.cellId && (
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <TowerControl size={16} />
                  <span>Cell ID</span>
                </div>
                <div className="glass p-3 rounded-md">
                  <p className="text-foreground font-mono">{event.cellId}</p>
                </div>
              </div>
            )}

            {/* Message */}
            {event.message && (
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <AlertTriangle size={16} />
                  <span>Message</span>
                </div>
                <div className="glass p-3 rounded-md">
                  <p className="text-foreground">{event.message}</p>
                </div>
              </div>
            )}
          </div>

          {/* Event-Specific Details */}
          {(event.type === 'call-placed' || event.type === 'call-dropped') && (
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground border-b border-border pb-2">
                Call Details
              </h3>
              
              {event.location && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <MapPin size={16} />
                    <span>Location</span>
                  </div>
                  <div className="glass p-3 rounded-md">
                    <p className="text-foreground font-mono">
                      Lat: {event.location.lat.toFixed(6)}, Lng: {event.location.lng.toFixed(6)}
                    </p>
                  </div>
                </div>
              )}

              {event.signalStrength && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Signal size={16} />
                    <span>Signal Strength</span>
                  </div>
                  <div className="glass p-3 rounded-md">
                    <p className="text-foreground font-mono">{event.signalStrength} dBm</p>
                  </div>
                </div>
              )}
            </div>
          )}

          {event.type === 'anomaly-detected' && (
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground border-b border-border pb-2">
                Anomaly Details
              </h3>
              
              {event.anomalyType && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <AlertTriangle size={16} />
                    <span>Anomaly Type</span>
                  </div>
                  <div className="glass p-3 rounded-md">
                    <p className="text-foreground">{event.anomalyType}</p>
                  </div>
                </div>
              )}

              {event.band && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Radio size={16} />
                    <span>Frequency Band</span>
                  </div>
                  <div className="glass p-3 rounded-md">
                    <p className="text-foreground font-mono">{event.band}</p>
                  </div>
                </div>
              )}

              {event.sourceId && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Database size={16} />
                    <span>Source ID</span>
                  </div>
                  <div className="glass p-3 rounded-md">
                    <p className="text-foreground font-mono">{event.sourceId}</p>
                  </div>
                </div>
              )}

              {event.recommendedFix && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Wrench size={16} />
                    <span>Recommended Fix</span>
                  </div>
                  <div className="glass p-4 rounded-md border border-primary/30 bg-primary/10">
                    <p className="text-foreground leading-relaxed text-sm">{event.recommendedFix}</p>
                  </div>
                </div>
              )}
            </div>
          )}

          {event.type === 'tower-up' && event.recoveryTime && (
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground border-b border-border pb-2">
                Recovery Details
              </h3>
              
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <CheckCircle size={16} />
                  <span>Recovery Time</span>
                </div>
                <div className="glass p-3 rounded-md">
                  <p className="text-foreground">{event.recoveryTime} minutes</p>
                </div>
              </div>
            </div>
          )}

          {/* Raw Event Data */}
          <details className="space-y-2">
            <summary className="cursor-pointer text-sm text-muted-foreground hover:text-foreground transition-colors">
              Technical Details (Raw Event Data)
            </summary>
            <div className="glass p-3 rounded-md mt-2">
              <pre className="text-xs text-muted-foreground font-mono whitespace-pre-wrap overflow-x-auto">
                {JSON.stringify(event, null, 2)}
              </pre>
            </div>
          </details>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default EventDetailsDialog;