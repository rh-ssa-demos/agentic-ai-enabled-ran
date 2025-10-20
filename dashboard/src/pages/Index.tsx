
import { useState } from "react";
import MapView from "../components/MapView";
import LiveFeed from "../components/LiveFeed";
import StatusHeader from "../components/StatusHeader";
import RanChatBox from "../components/RanChatBox";

import { useCallRecords } from "../hooks/useCallRecords";
import { useRemediationEvents } from "../hooks/useRemediationEvents";
import { useAnomalyEvents } from "../hooks/useAnomalyEvents";
import { useAnomalies } from "../hooks/useAnomalies";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { CircleAlert } from "lucide-react";

const Index = () => {
  // Use our custom hooks to fetch data
  const { towers, callEvents, avgRecoveryTime, isLoading: callsLoading, isError: callsError } = useCallRecords();
  const { events: remediationEvents, isLoading: remediationLoading, isError: remediationError } = useRemediationEvents();
  const { events: anomalyEvents, isLoading: anomalyLoading, isError: anomalyError } = useAnomalyEvents();
  const { anomalies, isLoading: anomaliesLoading, isError: anomaliesError } = useAnomalies();
  
  // Combine and sort all events
  const allEvents = [...callEvents, ...remediationEvents, ...anomalyEvents].sort((a, b) => 
    b.timestamp.getTime() - a.timestamp.getTime()
  );

  // Hardcoded Mapbox token - replace with your actual token
  const mapboxToken = "pk.eyJ1IjoidG9sYXJld2EiLCJhIjoiY21hYmd6d3c5MmRqOTJpbzlzbXo5amZrMCJ9.WH8s7dEAXdTf_u08Clikig";

  const isLoading = callsLoading || remediationLoading || anomalyLoading || anomaliesLoading;
  const isError = callsError || remediationError || anomalyError || anomaliesError;

  if (isError) {
    return (
      <div className="min-h-screen bg-background text-foreground p-8">
        <Alert variant="destructive">
          <CircleAlert className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>
            Failed to connect to the database. Please check your connection settings and try again.
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <div className="min-h-screen theme-bg text-foreground relative overflow-hidden">
      {/* Background elements for depth */}
      <div className="absolute inset-0 opacity-30">
        <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-blue-500/20 rounded-full blur-3xl"></div>
        <div className="absolute bottom-1/4 right-1/4 w-80 h-80 bg-purple-500/20 rounded-full blur-3xl"></div>
        <div className="absolute top-3/4 left-1/2 w-64 h-64 bg-cyan-500/20 rounded-full blur-3xl"></div>
      </div>
      
      <div className="relative z-10">
        <div className="px-4 py-2">
          <StatusHeader towers={towers} avgRecoveryTime={avgRecoveryTime} anomalies={anomalies} />
        </div>
        
        <main className="container mx-auto px-4 py-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="md:col-span-2 h-[500px] overflow-hidden rounded-lg glass-glow relative">
              <MapView towers={towers} mapboxToken={mapboxToken} />
              {isLoading && (
                <div className="absolute inset-0 flex items-center justify-center glass-dark rounded-lg">
                  <div className="text-center">
                    <div className="spinner h-8 w-8 rounded-full border-4 border-t-blue-400 border-b-blue-600 border-l-blue-500 border-r-blue-500 animate-spin mx-auto mb-4"></div>
                    <p className="text-foreground/90">Loading tower data...</p>
                  </div>
                </div>
              )}
            </div>
            
            <div className="md:col-span-1">
              <LiveFeed events={allEvents} />
            </div>
          </div>
          
          {/* RAN AI Chat Section */}
          <div className="mt-6">
            <RanChatBox />
          </div>
        </main>
      </div>
    </div>
  );
};

export default Index;
