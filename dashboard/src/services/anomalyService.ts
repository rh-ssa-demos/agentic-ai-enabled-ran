import { Event } from '../types/network';
import { dbConfig } from '../config/dbConfig';
import { toast } from '@/hooks/use-toast';

interface AnomalyRecord {
  cell_id: number;
  band: string;
  anomaly_type: string;
  anomaly: string;
  source_id: string;
  creation_date: string;
  recommended_fix?: string;
}

interface ProcessedAnomaly {
  cellId: string;
  count: number;
  types: string[];
  latestAnomaly: string;
  latestDate: string;
}

interface AnomalyFetchResult {
  anomalies: AnomalyRecord[];
  isUsingFallback: boolean;
  dataSource: string;
}

let anomalyCache: Map<string, ProcessedAnomaly> | null = null;
let lastFetchResult: AnomalyFetchResult | null = null;

export async function fetchAnomalies(): Promise<AnomalyFetchResult> {
  // Priority: default URL (with env variable fallback), then custom URL from localStorage
  const customUrl = typeof window !== 'undefined' ? localStorage.getItem('custom-anomalies-url') : null;
  const primaryUrl = dbConfig.anomaliesJsonUrl || customUrl;
  
  // Try primary URL first
  if (primaryUrl) {
    try {
      console.log('Fetching anomalies from:', primaryUrl);
      const response = await fetch(primaryUrl, { cache: 'no-store' });
      if (!response.ok) {
        throw new Error(`Failed to fetch anomalies: ${response.statusText}`);
      }
      
      // Check if response is actually JSON by looking at content-type or trying to parse
      const contentType = response.headers.get('content-type');
      if (contentType && !contentType.includes('application/json')) {
        throw new Error('Response is not JSON format');
      }
      
      const data = await response.json();
      // Validate that it's an array (expected format)
      if (!Array.isArray(data)) {
        throw new Error('Response is not an array of anomalies');
      }
      
      const result: AnomalyFetchResult = {
        anomalies: data,
        isUsingFallback: false,
        dataSource: primaryUrl
      };
      lastFetchResult = result;
      return result;
    } catch (error) {
      console.error('Error fetching anomalies from primary URL:', error);
      console.log('Falling back to local anomalies.json file');
    }
  }
  
  // Fallback to local file
  try {
    console.log('Fetching anomalies from local file: /anomalies.json');
    const response = await fetch('/anomalies.json', { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Failed to fetch local anomalies: ${response.statusText}`);
    }
    const data = await response.json();
    const result: AnomalyFetchResult = {
      anomalies: data,
      isUsingFallback: true,
      dataSource: '/anomalies.json'
    };
    lastFetchResult = result;
    return result;
  } catch (error) {
    console.error('Error fetching anomalies from local file:', error);
    toast({
      title: "Anomalies Loading Error",
      description: "Failed to load anomalies from both remote URL and local file. Check console for details.",
      variant: "destructive",
    });
    const result: AnomalyFetchResult = {
      anomalies: [],
      isUsingFallback: true,
      dataSource: 'error'
    };
    lastFetchResult = result;
    return result;
  }
}

export async function processAnomalies(): Promise<Map<string, ProcessedAnomaly>> {
  if (anomalyCache) {
    return anomalyCache;
  }

  const result = await fetchAnomalies();
  const anomalies = result.anomalies;
  const processedMap = new Map<string, ProcessedAnomaly>();

  anomalies.forEach(anomaly => {
    const cellId = anomaly.cell_id.toString();
    
    if (processedMap.has(cellId)) {
      const existing = processedMap.get(cellId)!;
      existing.count++;
      if (!existing.types.includes(anomaly.anomaly_type)) {
        existing.types.push(anomaly.anomaly_type);
      }
      // Update if this anomaly is more recent
      if (new Date(anomaly.creation_date) > new Date(existing.latestDate)) {
        existing.latestAnomaly = anomaly.anomaly;
        existing.latestDate = anomaly.creation_date;
      }
    } else {
      processedMap.set(cellId, {
        cellId,
        count: 1,
        types: [anomaly.anomaly_type],
        latestAnomaly: anomaly.anomaly,
        latestDate: anomaly.creation_date
      });
    }
  });

  anomalyCache = processedMap;
  return processedMap;
}

export function hasAnomaly(cellId: string, anomalyMap: Map<string, ProcessedAnomaly>): boolean {
  return anomalyMap.has(cellId);
}

export function getAnomalyInfo(cellId: string, anomalyMap: Map<string, ProcessedAnomaly>): ProcessedAnomaly | null {
  return anomalyMap.get(cellId) || null;
}

// Convert anomaly records to events for LiveFeed
export function convertAnomaliesToEvents(anomalies: AnomalyRecord[]): Event[] {
  return anomalies.map(anomaly => {
    // Parse timestamp as UTC and convert to local time
    let timestamp: Date;
    try {
      // If the timestamp ends with 'Z', it's already UTC
      if (anomaly.creation_date.endsWith('Z')) {
        timestamp = new Date(anomaly.creation_date);
      } else {
        // Assume it's UTC if no timezone info
        timestamp = new Date(anomaly.creation_date + 'Z');
      }
      
      // Validate the timestamp isn't in the future
      const now = new Date();
      if (timestamp > now) {
        console.warn('Anomaly timestamp is in the future, using current time:', anomaly.creation_date);
        timestamp = now;
      }
    } catch (error) {
      console.error('Invalid timestamp format:', anomaly.creation_date, error);
      timestamp = new Date(); // Use current time as fallback
    }
    
    return {
      type: "anomaly-detected" as const,
      timestamp,
      message: `${anomaly.anomaly_type}: ${anomaly.anomaly}`,
      towerId: anomaly.cell_id,
      cellId: anomaly.cell_id.toString(),
      // Add anomaly-specific data
      anomalyType: anomaly.anomaly_type,
      band: anomaly.band,
      sourceId: anomaly.source_id,
      recommendedFix: anomaly.recommended_fix
    };
  });
}

// Get fallback status
export function getAnomalyDataSource(): { isUsingFallback: boolean; dataSource: string } | null {
  return lastFetchResult ? {
    isUsingFallback: lastFetchResult.isUsingFallback,
    dataSource: lastFetchResult.dataSource
  } : null;
}

// Clear cache when needed
export function clearAnomalyCache(): void {
  anomalyCache = null;
  lastFetchResult = null;
}