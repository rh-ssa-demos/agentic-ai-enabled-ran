import { Tower } from "../types/network";
import { Progress } from "@/components/ui/progress";
import { getAnomalyDataSource } from "../services/anomalyService";
import { useEffect, useState } from "react";
import { Badge } from "./ui/badge";
import { AlertTriangle, CheckCircle } from "lucide-react";
import { ThemeToggle } from "./ThemeToggle";
interface ProcessedAnomaly {
  cellId: string;
  count: number;
  types: string[];
  latestAnomaly: string;
  latestDate: string;
}
interface StatusHeaderProps {
  towers: Tower[];
  avgRecoveryTime: number;
  anomalies: Map<string, ProcessedAnomaly>;
}
const StatusHeader: React.FC<StatusHeaderProps> = ({
  towers,
  avgRecoveryTime,
  anomalies
}) => {
  const [dataSource, setDataSource] = useState<{ isUsingFallback: boolean; dataSource: string } | null>(null);

  // Check data source status periodically
  useEffect(() => {
    const checkDataSource = () => {
      const source = getAnomalyDataSource();
      setDataSource(source);
    };
    
    checkDataSource();
    const interval = setInterval(checkDataSource, 5000);
    
    return () => clearInterval(interval);
  }, []);

  const totalTowers = towers.length;
  const activeTowers = towers.filter(tower => tower.status === "up").length;
  const towersWithAnomalies = towers.filter(tower => anomalies.has(tower.id.toString())).length;
  const healthyTowers = towers.filter(tower => tower.status === "up" && !anomalies.has(tower.id.toString())).length;
  const healthPercentage = totalTowers > 0 ? Math.floor(healthyTowers / totalTowers * 100) : 0;

  // Format recovery time in a human-readable way (input is now in seconds)
  const formatRecoveryTime = (seconds: number): string => {
    if (seconds === 0) return "N/A";
    if (seconds < 60) {
      return `${Math.round(seconds)} seconds`;
    }
    const minutes = seconds / 60;
    if (minutes < 60) {
      const wholeMinutes = Math.floor(minutes);
      const remainingSeconds = Math.round(seconds % 60);
      if (remainingSeconds === 0) {
        return wholeMinutes === 1 ? "1 minute" : `${wholeMinutes} minutes`;
      }
      return `${wholeMinutes}m ${remainingSeconds}s`;
    }
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = Math.round(minutes % 60);
    if (remainingMinutes === 0) {
      return hours === 1 ? "1 hour" : `${hours} hours`;
    }
    return `${hours}h ${remainingMinutes}m`;
  };
  return <header className="bg-card/50 border-b border-border py-4 backdrop-blur-lg">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between mb-4">
          <h1 className="text-2xl font-bold text-foreground drop-shadow-lg">Network Dashboard</h1>
          
          <div className="flex items-center gap-3">
            {dataSource && (
              <Badge 
                variant={dataSource.isUsingFallback ? "destructive" : "default"} 
                className="flex items-center gap-1"
              >
                {dataSource.isUsingFallback ? (
                  <>
                    <AlertTriangle size={14} />
                    Fallback Mode (Local JSON)
                  </>
                ) : (
                  <>
                    <CheckCircle size={14} />
                    Live
                  </>
                )}
              </Badge>
            )}
            <ThemeToggle />
          </div>
        </div>
        
        <div className="flex flex-wrap gap-6">
          <div className="stat-card">
            <div className="text-muted-foreground text-sm font-medium">Network Health</div>
            <div className="flex items-center mt-1">
              <div className={`text-2xl font-bold drop-shadow-md ${healthPercentage >= 90 ? 'text-green-400' : healthPercentage >= 70 ? 'text-yellow-400' : 'text-red-400'}`}>
                {healthPercentage}%
              </div>
              <div className="w-32 h-2 bg-muted rounded-full ml-3 overflow-hidden backdrop-blur-sm">
                <div className={`h-full rounded-full transition-all duration-500 ${healthPercentage >= 90 ? 'bg-green-400 shadow-green-400/50' : healthPercentage >= 70 ? 'bg-yellow-400 shadow-yellow-400/50' : 'bg-red-400 shadow-red-400/50'} shadow-lg`} style={{
                width: `${healthPercentage}%`
              }}></div>
              </div>
            </div>
          </div>
          
          <div className="stat-card">
            <div className="text-muted-foreground text-sm font-medium">Avg Recovery Time</div>
            <div className="text-2xl font-bold text-blue-400 drop-shadow-md">
              {formatRecoveryTime(avgRecoveryTime)}
            </div>
          </div>
          
          <div className="stat-card">
            <div className="text-muted-foreground text-sm font-medium">Total Towers</div>
            <div className="text-2xl font-bold text-foreground drop-shadow-md">{totalTowers}</div>
          </div>
          
          <div className="stat-card">
            <div className="text-muted-foreground text-sm font-medium">Healthy Towers</div>
            <div className="text-2xl font-bold text-green-400 drop-shadow-md">{healthyTowers}</div>
          </div>
          
          <div className="stat-card">
            <div className="text-muted-foreground text-sm font-medium">w/ Anomalies</div>
            <div className="text-2xl font-bold text-red-400 drop-shadow-md">{towersWithAnomalies}</div>
          </div>
        </div>
      </div>
    </header>;
};
export default StatusHeader;