import { fetchCallRecords, fetchRecentDroppedCalls, convertCallsToEvents } from './callService';
import { deriveTowersFromCalls, getAvgRecoveryTime } from './towerService';
import { dbConfig } from '../config/dbConfig';

// Always use mock data now
console.log('Using mock data for all database operations');

// Add a test connection function to check if the database is available
export async function testDatabaseConnection(): Promise<boolean> {
  console.log('Using mock data, database connection test skipped');
  return true; // Mock data is always available
}

// Fetch remediation events
export async function fetchRemediationEvents(limit: number = 50) {
  console.log('Using mock data for remediation events');
  return []; // Return empty array for mock
}

// Re-export all the functions from our service modules
export {
  fetchCallRecords,
  fetchRecentDroppedCalls,
  deriveTowersFromCalls,
  convertCallsToEvents,
  getAvgRecoveryTime
};

// Export placeholder subscription function for compatibility
export const subscribeToCallRecords = () => {
  console.log('Real-time subscriptions not available with mock data');
  return { unsubscribe: () => {} };
};