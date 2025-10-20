import { CallRecord, Event } from '../types/network';

// Function to fetch call records (returns empty array - no mock data)
export async function fetchCallRecords(limit: number = 100): Promise<CallRecord[]> {
  console.log('No call records available - mock data removed');
  return [];
}

// Function to fetch recent dropped calls (returns empty array - no mock data)
export async function fetchRecentDroppedCalls(minutes: number = 30): Promise<CallRecord[]> {
  console.log('No dropped calls available - mock data removed');
  return [];
}

// Convert call records to events for the LiveFeed
export function convertCallsToEvents(calls: CallRecord[]): Event[] {
  return calls.map((call) => {
    // Use cellId if available, otherwise fall back to a generated tower ID
    const cellId = call.cellId || `CELL_${Math.floor(Math.random() * 5) + 1}`;
    const towerId = Math.floor(Math.random() * 5) + 1; // Keep for backward compatibility
    
    // Ensure timestamp is a proper Date object
    const timestamp = call.timestamp instanceof Date ? call.timestamp : new Date(call.timestamp);
    
    return {
      type: call.isDropped ? "call-dropped" : "call-placed",
      timestamp: timestamp,
      message: "", // Empty message for both call types - we'll display details separately
      towerId,
      cellId,
      location: call.location,
      signalStrength: call.signalStrength
    };
  });
}