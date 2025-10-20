import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import { Event } from '../types/network';
import { fetchRemediationEvents } from '../services/dbService';
import { dbConfig } from '../config/dbConfig';

export function useRemediationEvents(limit: number = 50) {
  const queryClient = useQueryClient();

  // Fetch remediation events
  const remediationEventsQuery = useQuery({
    queryKey: ['remediationEvents'],
    queryFn: () => fetchRemediationEvents(limit),
    refetchInterval: 10000, // Poll every 10 seconds with mock data
  });

  // No real-time subscriptions with mock data
  useEffect(() => {
    console.log('Real-time subscriptions not available with mock data');
  }, [queryClient, limit]);

  // Transform data to Event format
  const events: Event[] = remediationEventsQuery.data?.map((event: any) => ({
    id: event.id || Math.random().toString(),
    type: event.event_type || 'remediation-started',
    timestamp: new Date(event.event_time || Date.now()),
    message: event.description || 'Remediation event',
    towerId: event.towerId || Math.floor(Math.random() * 5) + 1,
    cellId: event.cellId,
  })) || [];

  return {
    events,
    isLoading: remediationEventsQuery.isLoading,
    isError: remediationEventsQuery.isError,
    error: remediationEventsQuery.error,
  };
}