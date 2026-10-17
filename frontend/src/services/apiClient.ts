export const BACKEND_NODES = [
  'http://localhost:8001',
  'http://localhost:8002',
  'http://localhost:8003'
];

export const NODE_ID_MAP: Record<string, string> = {
  'node-1': 'http://localhost:8001',
  'node-2': 'http://localhost:8002',
  'node-3': 'http://localhost:8003'
};

export const PORT_TO_NODE_ID: Record<number, string> = {
  8001: 'Node 1',
  8002: 'Node 2',
  8003: 'Node 3'
};

export interface ConnectionState {
  currentNodeUrl: string;
  currentNodeName: string;
  isFailover: boolean;
  statusMessage: string | null;
  allNodesOffline: boolean;
}

type Listener = (state: ConnectionState) => void;

class ApiClient {
  private nodeUrls: string[] = BACKEND_NODES;
  private preferredIndex: number = 0;
  private initialNodeUrl: string = BACKEND_NODES[0];
  private isFailoverState: boolean = false;
  private listeners: Listener[] = [];
  private lastNotification: string | null = null;
  private allNodesOffline: boolean = false;

  public subscribe(listener: Listener) {
    this.listeners.push(listener);
    listener(this.getState());
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  private notify() {
    const state = this.getState();
    this.listeners.forEach((l) => l(state));
  }

  public getState(): ConnectionState {
    const url = this.getPreferredNodeUrl();
    const portMatch = url.match(/:(\d+)/);
    const port = portMatch ? parseInt(portMatch[1], 10) : 8001;
    const nodeName = PORT_TO_NODE_ID[port] || `Port ${port}`;

    return {
      currentNodeUrl: url,
      currentNodeName: nodeName,
      isFailover: this.isFailoverState,
      statusMessage: this.lastNotification,
      allNodesOffline: this.allNodesOffline
    };
  }

  public getPreferredNodeUrl(): string {
    return this.nodeUrls[this.preferredIndex];
  }

  public setPreferredNodeUrl(url: string, reason?: string) {
    const idx = this.nodeUrls.indexOf(url);
    if (idx !== -1) {
      if (this.preferredIndex !== idx || this.allNodesOffline) {
        const oldPort = this.nodeUrls[this.preferredIndex].match(/:(\d+)/)?.[1];
        const newPort = url.match(/:(\d+)/)?.[1];
        this.preferredIndex = idx;
        this.isFailoverState = url !== this.initialNodeUrl;
        this.allNodesOffline = false;
        if (reason) {
          this.lastNotification = reason;
        } else if (oldPort !== newPort) {
          const targetNodeName = PORT_TO_NODE_ID[parseInt(newPort || '8001', 10)] || `Node at port ${newPort}`;
          this.lastNotification = `Backend connection switched to ${targetNodeName}`;
        }
        this.notify();
      }
    }
  }

  public clearNotification() {
    this.lastNotification = null;
    this.notify();
  }

  /**
   * Centralized HTTP fetch with automatic node failover and NOT_LEADER discovery.
   */
  public async fetch(path: string, init: RequestInit = {}): Promise<Response> {
    let attempts = 0;
    const maxAttempts = this.nodeUrls.length;

    let currentIndex = this.preferredIndex;

    while (attempts < maxAttempts) {
      const nodeUrl = this.nodeUrls[currentIndex];
      const targetUrl = `${nodeUrl}${path.startsWith('/') ? path : '/' + path}`;

      try {
        const fetchOptions: RequestInit = {
          ...init,
          credentials: 'include'
        };

        const response = await fetch(targetUrl, fetchOptions);

        // Check for NOT_LEADER structured error response
        if (!response.ok && response.status === 503) {
          const clonedResp = response.clone();
          try {
            const errorBody = await clonedResp.json();
            if (errorBody?.detail?.error === 'NOT_LEADER' && errorBody?.detail?.leader_id) {
              const leaderId = errorBody.detail.leader_id;
              const leaderUrl = NODE_ID_MAP[leaderId];
              if (leaderUrl && leaderUrl !== nodeUrl) {
                console.log(`[ApiClient] NOT_LEADER received from ${nodeUrl}. Switching to Leader ${leaderId} (${leaderUrl})`);
                const leaderName = PORT_TO_NODE_ID[parseInt(leaderUrl.match(/:(\d+)/)?.[1] || '8001', 10)];
                this.setPreferredNodeUrl(leaderUrl, `Raft leader redirected request to ${leaderName}`);
                // Retry request against the reported leader
                currentIndex = this.nodeUrls.indexOf(leaderUrl);
                attempts++;
                continue;
              }
            }
          } catch {
            // Not a JSON NOT_LEADER error, continue normal response handling
          }
        }

        // If request succeeded or returned non-503 response
        if (this.preferredIndex !== currentIndex) {
          const newPort = nodeUrl.match(/:(\d+)/)?.[1];
          const targetNodeName = PORT_TO_NODE_ID[parseInt(newPort || '8001', 10)] || `Node at port ${newPort}`;
          this.setPreferredNodeUrl(nodeUrl, `Backend connection switched to ${targetNodeName}`);
        }

        this.allNodesOffline = false;
        return response;

      } catch (err: any) {
        console.warn(`[ApiClient] Network error reaching ${nodeUrl}${path}:`, err.message || err);
        attempts++;
        if (attempts < maxAttempts) {
          // Rotate to next candidate node
          currentIndex = (currentIndex + 1) % this.nodeUrls.length;
          const nextPort = this.nodeUrls[currentIndex].match(/:(\d+)/)?.[1];
          console.log(`[ApiClient] Failing over to candidate node at port ${nextPort}...`);
        }
      }
    }

    // All backend nodes failed
    this.allNodesOffline = true;
    this.lastNotification = 'All backend nodes are currently unavailable.';
    this.notify();
    throw new TypeError('All backend nodes are currently unavailable.');
  }
}

export const apiClient = new ApiClient();
