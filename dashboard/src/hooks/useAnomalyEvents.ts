import { useQuery } from '@tanstack/react-query';
import { fetchAnomalies, convertAnomaliesToEvents } from '../services/anomalyService';
import { Event } from '../types/network';

export function useAnomalyEvents() {
  const anomalyEventsQuery = useQuery({
    queryKey: ['anomalyEvents'],
    queryFn: async (): Promise<Event[]> => {
      const result = await fetchAnomalies();
      return convertAnomaliesToEvents(result.anomalies);
    },
    staleTime: 5 * 60 * 1000, // Consider data fresh for 5 minutes
    refetchInterval: 5000, // Refetch every 5 seconds
  });

  return {
    events: anomalyEventsQuery.data || [],
    isLoading: anomalyEventsQuery.isLoading,
    isError: anomalyEventsQuery.isError,
    error: anomalyEventsQuery.error,
  };
}