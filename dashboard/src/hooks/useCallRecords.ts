
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import { CallRecord, Event, Tower } from '../types/network';
import { 
  fetchCallRecords, 
  deriveTowersFromCalls, 
  convertCallsToEvents, 
  getAvgRecoveryTime,
  subscribeToCallRecords
} from '../services/dbService';
import { dbConfig } from '../config/dbConfig';

export function useCallRecords(limit: number = 100) {
  const queryClient = useQueryClient();

  // Fetch call records
  const callsQuery = useQuery({
    queryKey: ['callRecords', limit],
    queryFn: () => fetchCallRecords(limit),
    refetchInterval: 5000, // Poll every 5 seconds with mock data
  });

  // Derive towers from call patterns
  const towersQuery = useQuery({
    queryKey: ['towers'],
    queryFn: deriveTowersFromCalls,
    refetchInterval: dbConfig.useTowersJson
      ? (dbConfig.towersJsonPollMs || 15000)
      : 10000, // Poll JSON at configured interval, otherwise default
  });
  
  // Get average recovery time
  const recoveryTimeQuery = useQuery({
    queryKey: ['recoveryTime'],
    queryFn: getAvgRecoveryTime,
    refetchInterval: 30000, // Poll every 30 seconds with mock data
  });

  // No real-time subscriptions with mock data
  useEffect(() => {
    // Mock data doesn't support real-time subscriptions
    console.log('Real-time subscriptions not available with mock data');
  }, [queryClient, limit]);

  // Convert call records to events for LiveFeed, ensuring we have valid timestamps
  const callEvents: Event[] = callsQuery.data ? convertCallsToEvents(callsQuery.data) : [];

  return {
    calls: callsQuery.data || [],
    towers: towersQuery.data || [],
    callEvents,
    avgRecoveryTime: recoveryTimeQuery.data || 0,
    isLoading: callsQuery.isLoading || towersQuery.isLoading,
    isError: callsQuery.isError || towersQuery.isError,
    error: callsQuery.error || towersQuery.error,
  };
}
