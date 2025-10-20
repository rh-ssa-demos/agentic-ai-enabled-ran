
import React, { useEffect, useRef } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import { Tower } from '../types/network';
import { useAnomalies } from '../hooks/useAnomalies';
import { hasAnomaly, getAnomalyInfo } from '../services/anomalyService';
import { useTheme } from 'next-themes';

interface MapViewProps {
  towers: Tower[];
  mapboxToken: string;
}

const MapView: React.FC<MapViewProps> = ({ towers, mapboxToken }) => {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);
  const markersRef = useRef<{ [key: number]: mapboxgl.Marker }>({});
  const { anomalies } = useAnomalies();
  const { theme, resolvedTheme } = useTheme();

  // Calculate bounds from towers data
  const calculateTowerBounds = (towers: Tower[]) => {
    if (towers.length === 0) {
      // Fallback to Dallas-Fort Worth area if no towers
      return {
        center: [-96.85, 33.05] as [number, number],
        zoom: 10
      };
    }

    const lats = towers.map(tower => tower.lat);
    const lngs = towers.map(tower => tower.lng);
    
    const bounds = {
      north: Math.max(...lats),
      south: Math.min(...lats),
      east: Math.max(...lngs),
      west: Math.min(...lngs)
    };

    const center: [number, number] = [
      (bounds.west + bounds.east) / 2,
      (bounds.south + bounds.north) / 2
    ];

    // Calculate zoom level based on the span of coordinates
    const latSpan = bounds.north - bounds.south;
    const lngSpan = bounds.east - bounds.west;
    const maxSpan = Math.max(latSpan, lngSpan);
    
    // Zoom level calculation: smaller spans need higher zoom
    let zoom = 10;
    if (maxSpan < 0.1) zoom = 13;
    else if (maxSpan < 0.3) zoom = 11;
    else if (maxSpan < 0.7) zoom = 10;
    else if (maxSpan < 1.5) zoom = 9;
    else zoom = 8;

    return { center, zoom };
  };

  useEffect(() => {
    if (!mapContainer.current || !mapboxToken) return;

    // Calculate initial map view based on towers
    const { center, zoom } = calculateTowerBounds(towers);

    // Initialize map
    mapboxgl.accessToken = mapboxToken;
    
    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: resolvedTheme === 'light' ? 'mapbox://styles/mapbox/light-v11' : 'mapbox://styles/mapbox/dark-v11',
      center: center,
      zoom: zoom,
    });

    // Add navigation controls
    map.current.addControl(
      new mapboxgl.NavigationControl(),
      'top-right'
    );

    // Add custom popup styles to the document
    if (!document.getElementById('mapbox-popup-styles')) {
      const styleElement = document.createElement('style');
      styleElement.id = 'mapbox-popup-styles';
      styleElement.textContent = `
        .mapboxgl-popup-content {
          background: rgba(31, 41, 55, 0.95);
          color: white;
          padding: 12px;
          border-radius: 8px;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
          border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .mapboxgl-popup-close-button {
          color: white;
          font-size: 16px;
        }
        .mapboxgl-popup-tip {
          border-top-color: rgba(31, 41, 55, 0.95);
        }
      `;
      document.head.appendChild(styleElement);
    }

    // Clean up on unmount
    return () => {
      if (map.current) {
        map.current.remove();
      }
      // Remove custom styles when component unmounts
      const styleElement = document.getElementById('mapbox-popup-styles');
      if (styleElement) {
        document.head.removeChild(styleElement);
      }
    };
  }, [mapboxToken, towers]);

  // Fit map to towers bounds when towers data changes
  useEffect(() => {
    if (!map.current || towers.length === 0) return;

    const { center, zoom } = calculateTowerBounds(towers);
    
    // Smoothly animate to the new view
    map.current.easeTo({
      center: center,
      zoom: zoom,
      duration: 1000
    });
  }, [towers]);

  // Update map style when theme changes
  useEffect(() => {
    if (!map.current) return;
    
    const newStyle = resolvedTheme === 'light' ? 'mapbox://styles/mapbox/light-v11' : 'mapbox://styles/mapbox/dark-v11';
    map.current.setStyle(newStyle);
  }, [resolvedTheme]);

  // Update markers when towers change
  useEffect(() => {
    if (!map.current) return;

    // Wait for map to load before adding markers
    const addMarkers = () => {
      // Create a set of tower IDs from current data for efficient lookup
      const currentTowerIds = new Set(towers.map(tower => tower.id));
      
      // Remove markers for towers that are no longer present in the data
      Object.keys(markersRef.current).forEach(idString => {
        const id = parseInt(idString, 10);
        if (!currentTowerIds.has(id)) {
          // Tower no longer exists in the data, remove its marker
          markersRef.current[id].remove();
          delete markersRef.current[id];
        }
      });

      // Add or update markers for current towers
      towers.forEach(tower => {
        // Determine marker color based on tower status, anomalies, and dropped calls
        let markerColor = '#4ade80'; // Default green for up status
        let shadowColor = 'rgba(74, 222, 128, 0.6)';
        
        // Check for anomalies first (highest priority)
        const towerId = tower.id.toString();
        if (hasAnomaly(towerId, anomalies)) {
          markerColor = '#f87171'; // Red for anomalies
          shadowColor = 'rgba(248, 113, 113, 0.6)';
        } else if (tower.status === 'down') {
          markerColor = '#f87171'; // Red for down status
          shadowColor = 'rgba(248, 113, 113, 0.6)';
        } else if (tower.dropRate && tower.dropRate >= 2) {
          markerColor = '#facc15'; // Yellow for high drop rate but not down
          shadowColor = 'rgba(250, 204, 21, 0.6)';
        }

        // Check if we need to update or create a new marker
        if (markersRef.current[tower.id]) {
          // Update existing marker position
          markersRef.current[tower.id].setLngLat([tower.lng, tower.lat]);
          
          // We'll remove and recreate the marker element to update its appearance
          // This ensures that color changes are reflected
          markersRef.current[tower.id].remove();
        }

        // Create custom marker element
        const el = document.createElement('div');
        el.className = 'tower-marker';
        el.style.width = '24px';
        el.style.height = '24px';
        el.style.borderRadius = '50%';
        el.style.backgroundColor = markerColor;
        el.style.boxShadow = `0 0 10px 3px ${shadowColor}`;
        el.style.border = '2px solid white';
        el.style.cursor = 'pointer';

        // Create popup with tower information using inline styles instead of Tailwind classes
        const bandsHtml = tower.bands && tower.bands.length > 0 ? 
          `<div style="margin: 8px 0;">
            <p style="margin: 2px 0; font-weight: bold;">Frequency Bands:</p>
            <div style="display: flex; flex-wrap: wrap; gap: 4px; margin-top: 4px;">
              ${tower.bands.map(band => 
                `<span style="background: #3b82f6; color: white; padding: 2px 6px; border-radius: 12px; font-size: 11px;">${band}</span>`
              ).join('')}
            </div>
          </div>` : '';

        // Create anomaly information HTML
        const anomalyInfo = getAnomalyInfo(towerId, anomalies);
        const anomalyHtml = anomalyInfo ? 
          `<div style="margin: 8px 0; padding: 8px; background: rgba(248, 113, 113, 0.1); border-radius: 4px; border-left: 3px solid #f87171;">
            <p style="margin: 2px 0; font-weight: bold; color: #f87171;">⚠️ Network Anomalies (${anomalyInfo.count})</p>
            <p style="margin: 2px 0; font-size: 12px;">Types: ${anomalyInfo.types.join(', ')}</p>
            <p style="margin: 2px 0; font-size: 11px; color: #9ca3af;">Latest: ${anomalyInfo.latestAnomaly}</p>
          </div>` : '';
        
        const popup = new mapboxgl.Popup({ offset: 25 }).setHTML(
          `<div>
            <h3 style="font-weight: bold; margin-bottom: 4px;">${tower.name}</h3>
            ${tower.city ? `<p style="margin: 2px 0; font-size: 12px; color: #9ca3af;">City: ${tower.city}</p>` : ''}
            ${tower.adjacentCells && tower.adjacentCells.length > 0 ? 
              `<p style="margin: 2px 0; font-size: 12px; color: #9ca3af;">Adjacent Cells: ${tower.adjacentCells.join(', ')}</p>` : ''}
            ${anomalyHtml}
            <p style="margin: 4px 0;">Status: <span style="color: ${
              tower.status === 'up' ? '#4ade80' : '#f87171'
            };">${tower.status.toUpperCase()}</span></p>
            ${tower.maxCapacity ? 
              `<p style="margin: 4px 0;">Capacity: ${tower.maxCapacity}</p>` : ''}
            ${tower.dropRate !== undefined ? 
              `<p style="margin: 4px 0;">Drop Rate: <span style="${
                tower.dropRate >= 10 ? 'color: #f87171; font-weight: bold;' : 
                tower.dropRate >= 5 ? 'color: #facc15; font-weight: bold;' : ''
              }">${tower.dropRate.toFixed(1)}%</span></p>` : ''}
            ${tower.droppedCalls ? 
              `<p style="margin: 4px 0;">Dropped Calls: ${tower.droppedCalls}</p>` : ''}
            ${bandsHtml}
            <p style="margin: 4px 0; font-size: 12px; color: #9ca3af;">Location: ${tower.lat.toFixed(4)}, ${tower.lng.toFixed(4)}</p>
          </div>`
        );

        // Add marker to map
        const marker = new mapboxgl.Marker(el)
          .setLngLat([tower.lng, tower.lat])
          .setPopup(popup)
          .addTo(map.current!);

        // Store marker reference
        markersRef.current[tower.id] = marker;
      });
    };

    if (map.current.loaded()) {
      addMarkers();
    } else {
      map.current.on('load', addMarkers);
    }

  }, [towers, anomalies, map.current]);

  return (
    <div className="relative w-full h-full">
      <div ref={mapContainer} className="absolute inset-0" />
    </div>
  );
};

export default MapView;
