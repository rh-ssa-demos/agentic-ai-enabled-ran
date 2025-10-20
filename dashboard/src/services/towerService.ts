import { Tower } from '../types/network';
import { dbConfig } from '../config/dbConfig';
import { toast } from '@/hooks/use-toast';

// Load towers from JSON (local or remote)
async function loadTowersFromJson(): Promise<Tower[]> {
  try {
    // Check for custom URL in localStorage first, then config, then default
    const customUrl = typeof window !== 'undefined' ? localStorage.getItem('custom-json-url') : null;
    const url = customUrl || dbConfig.towersJsonUrl;
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error(`Failed to fetch towers JSON: ${res.status}`);
    const json = await res.json();
    const cells = Array.isArray(json) ? json : json.cells || [];
    return cells
      .map((c: any) => ({
        id: c.cell_id ?? c.id,
        name: c.name ?? `Cell ${c.cell_id ?? c.id}`,
        lat: Number(c.lat),
        lng: Number(c.lon ?? c.lng),
        status: (c.status === 'down' || c.max_capacity === 0) ? 'down' as const : 'up' as const,
        bands: c.bands || [],
        city: c.city,
        areaType: c.area_type,
        maxCapacity: c.max_capacity,
        adjacentCells: c.adjacent_cells || [],
      }))
      .filter((t: Tower) => Number.isFinite(t.lat) && Number.isFinite(t.lng));
  } catch (error) {
    console.error('Error loading towers from JSON:', error);
    toast({
      title: "JSON Loading Error",
      description: "Failed to load towers from JSON file. Check console for details.",
      variant: "destructive",
    });
    return [];
  }
}

// Get towers based on JSON source only (mock call data removed)
export async function deriveTowersFromCalls(): Promise<Tower[]> {
  // Check for localStorage override first
  const useJsonOverride = typeof window !== 'undefined' && localStorage.getItem('override-use-json') === 'true';
  const shouldUseJson = useJsonOverride || dbConfig.useTowersJson || dbConfig.towersJsonUrl;
  
  if (shouldUseJson) {
    console.log('Loading towers from JSON source');
    return await loadTowersFromJson();
  }

  console.log('No call data available - mock data removed. Only JSON towers supported.');
  return [];
}

// Get recovery time statistics (returns 0 since no call data available)
export async function getAvgRecoveryTime(): Promise<number> {
  console.log('No recovery time data available - mock data removed');
  return 0;
}