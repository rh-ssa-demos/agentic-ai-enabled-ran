import { useQuery } from '@tanstack/react-query';
import { processAnomalies } from '../services/anomalyService';

export function useAnomalies() {
  const anomaliesQuery = useQuery({
    queryKey: ['anomalies'],
    queryFn: processAnomalies,
    staleTime: 5 * 60 * 1000, // Consider data fresh for 5 minutes
    refetchInterval: 5000, // Refetch every 5 seconds
  });

  return {
    anomalies: anomaliesQuery.data || new Map(),
    isLoading: anomaliesQuery.isLoading,
    isError: anomaliesQuery.isError,
    error: anomaliesQuery.error,
  };
}