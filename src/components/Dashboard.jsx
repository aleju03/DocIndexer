import { useState, useEffect } from 'react';
import { BarChart3, Thermometer, Zap, AlertTriangle, RefreshCw, Server, Activity } from 'lucide-react';

const BACKEND_URL = 'http://localhost:8000';

const Dashboard = () => {
  const [workerStatuses, setWorkerStatuses] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const fetchWorkerStatuses = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch(`${BACKEND_URL}/workers/status/`);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setWorkerStatuses(data.workers || []);
      setLastUpdated(new Date());
    } catch (e) {
      console.error("Failed to fetch worker statuses:", e);
      setError(e.message);
      setWorkerStatuses([]);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchWorkerStatuses(); // Initial fetch
    // Set up an interval to auto-refresh every 2 seconds
    const intervalId = setInterval(fetchWorkerStatuses, 2000);
    // Clear the interval when the component unmounts
    return () => clearInterval(intervalId);
  }, []); // Empty dependency array means this effect runs once on mount and cleanup on unmount

  const formatPercentage = (value) => {
    return value !== null && value !== undefined ? `${value.toFixed(2)}%` : 'N/A';
  };

  // Function to get color based on percentage value
  const getColorClass = (percentage, type = 'cpu') => {
    if (percentage === null || percentage === undefined) return 'bg-gray-200';
    
    if (type === 'cpu') {
      if (percentage < 30) return 'bg-green-500';
      if (percentage < 70) return 'bg-yellow-500';
      return 'bg-red-500';
    } else {
      // RAM coloring - more tolerant
      if (percentage < 50) return 'bg-green-500';
      if (percentage < 80) return 'bg-yellow-500';
      return 'bg-red-500';
    }
  };

  return (
    <div className="w-full p-4 bg-gray-50 dark:bg-gray-800 rounded-lg shadow-md">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-xl font-semibold text-gray-800 dark:text-white flex items-center">
          <Server size={24} className="mr-2 text-blue-600 dark:text-blue-400" /> 
          Worker Node Status
        </h2>
        <div className="flex items-center">
          {lastUpdated && !error && (
            <span className="text-xs text-gray-500 dark:text-gray-400 mr-3">
              Last updated: {lastUpdated.toLocaleTimeString()}
            </span>
          )}
          <button
            onClick={fetchWorkerStatuses}
            disabled={isLoading}
            className="p-2 bg-blue-500 text-white rounded-full hover:bg-blue-600 disabled:bg-blue-300 flex items-center justify-center shadow-md text-sm transition-colors cursor-pointer"
            title="Refresh Status"
          >
            {isLoading ? (
              <RefreshCw size={18} className="animate-spin" />
            ) : (
              <RefreshCw size={18} />
            )}
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-100 dark:bg-red-900/30 border border-red-400 dark:border-red-800 text-red-700 dark:text-red-300 rounded-md flex items-center">
          <AlertTriangle size={20} className="mr-2 flex-shrink-0" />
          <p>Error fetching worker statuses: {error}</p>
        </div>
      )}

      {!isLoading && !error && workerStatuses.length === 0 && (
        <div className="text-center py-12 bg-white dark:bg-gray-700 rounded-lg shadow-sm border border-gray-200 dark:border-gray-600">
          <Activity size={48} className="mx-auto mb-4 text-gray-400 dark:text-gray-500" />
          <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">No Worker Nodes Found</h3>
          <p className="text-gray-500 dark:text-gray-400">No worker nodes are currently reporting their status.</p>
        </div>
      )}

      {workerStatuses.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {workerStatuses.map((worker) => (
            <div 
              key={worker.worker_id} 
              className="bg-white dark:bg-gray-700 p-5 rounded-lg shadow-sm border border-gray-200 dark:border-gray-600 hover:shadow-md transition-shadow duration-200"
            >
              <div className="flex items-start justify-between mb-3">
                <h3 className="text-md font-semibold text-blue-700 dark:text-blue-400 truncate max-w-[80%]" title={worker.worker_id}> 
                  {worker.worker_id.length > 20 ? `${worker.worker_id.substring(0,20)}...` : worker.worker_id}
                </h3>
                <div className="flex-shrink-0 px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-300 text-xs font-medium">
                  Active
                </div>
              </div>
              
              <div className="space-y-4">
                {/* CPU Usage with visual meter */}
                <div>
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-sm text-gray-600 dark:text-gray-300 flex items-center">
                      <BarChart3 size={16} className="mr-2 text-green-500" /> CPU Usage
                    </span>
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-200">
                      {formatPercentage(worker.cpu_percent)}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 dark:bg-gray-600 rounded-full h-2.5">
                    <div 
                      className={`h-2.5 rounded-full ${getColorClass(worker.cpu_percent, 'cpu')}`}
                      style={{ width: `${worker.cpu_percent !== null && worker.cpu_percent !== undefined ? Math.min(100, worker.cpu_percent) : 0}%` }}
                    ></div>
                  </div>
                </div>
                
                {/* RAM Usage with visual meter */}
                <div>
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-sm text-gray-600 dark:text-gray-300 flex items-center">
                      <Thermometer size={16} className="mr-2 text-orange-500" /> RAM Usage
                    </span>
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-200">
                      {formatPercentage(worker.ram_percent)}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 dark:bg-gray-600 rounded-full h-2.5">
                    <div 
                      className={`h-2.5 rounded-full ${getColorClass(worker.ram_percent, 'ram')}`}
                      style={{ width: `${worker.ram_percent !== null && worker.ram_percent !== undefined ? Math.min(100, worker.ram_percent) : 0}%` }}
                    ></div>
                  </div>
                </div>
                
                {/* Queue Length */}
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600 dark:text-gray-300 flex items-center">
                    <Zap size={16} className="mr-2 text-purple-500" /> Queue
                  </span>
                  <span className="px-2.5 py-0.5 bg-purple-100 dark:bg-purple-900/30 text-purple-800 dark:text-purple-300 rounded-full text-xs font-medium">
                    {worker.queue_length !== null && worker.queue_length !== undefined ? worker.queue_length : 'N/A'}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default Dashboard; 