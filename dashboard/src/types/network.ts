
export type TowerStatus = "up" | "down";

export interface Tower {
  id: number;
  name: string;
  lat: number;
  lng: number;
  status: TowerStatus;
  downSince?: Date; // Add timestamp for when tower went down
  droppedCalls?: number; // Track number of dropped calls
  dropRate?: number; // Add drop rate as a percentage
  bands?: string[]; // Frequency bands supported by the tower
  city?: string; // City where the tower is located
  areaType?: string; // Area type (industrial, residential, etc.)
  maxCapacity?: number; // Maximum capacity of the tower
  adjacentCells?: number[]; // Adjacent cell IDs
}

export type EventType = 
  | "call-placed" 
  | "call-dropped" 
  | "tower-up" 
  | "tower-down" 
  | "alert-triggered" 
  | "alert-resolved"
  | "remediation-started"
  | "remediation-completed"
  | "anomaly-detected";

export interface Event {
  type: EventType;
  timestamp: Date;
  message: string;
  towerId: number;
  cellId?: string;
  recoveryTime?: number; // Add recovery time in minutes (for tower-up events)
  location?: { lat: number; lng: number }; // Add location for call events
  signalStrength?: number; // Add signal strength for call events
  anomalyType?: string; // Add anomaly type for anomaly events
  band?: string; // Add band for anomaly events
  sourceId?: string; // Add source ID for anomaly events
  recommendedFix?: string; // Add recommended fix for anomaly events
}

export interface CallRecord {
  timestamp: Date;
  location: {
    lat: number;
    lng: number;
  };
  signalStrength: number;
  isDropped: boolean;
  cellId?: string;
}

// Helper interface to track tower activity based on call records
export interface TowerActivity {
  totalCalls: number;
  droppedCalls: number;
  lastActivity: Date;
}
