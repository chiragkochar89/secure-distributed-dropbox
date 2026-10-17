import { useState, useEffect } from 'react';
import { apiClient, ConnectionState } from './services/apiClient';

// SVG Icon Components
function ServerIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <rect width="20" height="8" x="2" y="2" rx="2" ry="2"/>
      <rect width="20" height="8" x="2" y="14" rx="2" ry="2"/>
      <line x1="6" x2="6.01" y1="6" y2="6"/>
      <line x1="6" x2="6.01" y1="18" y2="18"/>
    </svg>
  );
}

function ActivityIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
    </svg>
  );
}

function ShieldCheckIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/>
      <path d="m9 12 2 2 4-4"/>
    </svg>
  );
}

function FolderIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L8.6 3.3A2 2 0 0 0 6.9 2H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2z"/>
    </svg>
  );
}

function UploadIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/>
    </svg>
  );
}

function DownloadIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" x2="12" y1="15" y2="3"/>
    </svg>
  );
}

function TrashIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>
    </svg>
  );
}

function EditIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M12 20h9"/><path d="M16.376 3.622a1 1 0 0 1 3.002 3.002L7.368 18.635a2 2 0 0 1-.855.506l-2.872.838a.5.5 0 0 1-.62-.62l.838-2.872a2 2 0 0 1 .506-.854z"/>
    </svg>
  );
}

// Interfaces
interface User {
  id: string;
  email: string;
  username: string;
  role: string;
}

interface FileItem {
  id: string;
  owner_id: string;
  original_filename: string;
  sanitized_name: string;
  content_type: string;
  size_bytes: number;
  current_sha256: string;
  current_version: number;
  created_at: string;
  updated_at: string;
}

export interface NodeHealth {
  status: string;
  node_id: string;
  service: string;
  phase: string;
  http_port: number;
  grpc_port: number;
  peers: string[];
}

const DEFAULT_NODES = [
  { id: 'node-1', port: 8001, grpcPort: 50051 },
  { id: 'node-2', port: 8002, grpcPort: 50052 },
  { id: 'node-3', port: 8003, grpcPort: 50053 },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<'files' | 'nodes'>('files');
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  
  // Connection State & Failover
  const [connState, setConnState] = useState<ConnectionState>(apiClient.getState());

  // Auth Form State
  const [authMode, setAuthMode] = useState<'login' | 'register'>('login');
  const [email, setEmail] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [authError, setAuthError] = useState('');
  
  // Files State
  const [files, setFiles] = useState<FileItem[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [renameId, setRenameId] = useState<string | null>(null);
  const [newFilename, setNewFilename] = useState('');

  // Nodes Polling & System Diagnostics State
  const [nodeData, setNodeData] = useState<Record<string, { health: any | null; error: string | null }>>({});
  const [lastUpdated, setLastUpdated] = useState<string>('');

  // 0. Subscribe to apiClient connection state updates
  useEffect(() => {
    const unsubscribe = apiClient.subscribe(setConnState);
    return unsubscribe;
  }, []);

  // 1. Check current logged-in user on mount
  useEffect(() => {
    fetchMe();
  }, []);

  // 2. Fetch files when authenticated
  useEffect(() => {
    if (currentUser) {
      fetchFiles();
    }
  }, [currentUser]);

  // 3. Poll cluster nodes for system status
  useEffect(() => {
    fetchClusterHealth();
    const interval = setInterval(fetchClusterHealth, 3000);
    return () => clearInterval(interval);
  }, []);

  const fetchMe = async () => {
    try {
      const res = await apiClient.fetch('/api/v1/auth/me');
      if (res.ok) {
        const data: User = await res.json();
        setCurrentUser(data);
      } else {
        setCurrentUser(null);
      }
    } catch {
      setCurrentUser(null);
    }
  };

  const handleAuthSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setAuthError('');
    const endpoint = authMode === 'register' ? '/api/v1/auth/register' : '/api/v1/auth/login';
    const body = authMode === 'register' ? { email, username, password } : { username_or_email: username || email, password };

    try {
      const res = await apiClient.fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        let errorMsg = 'Authentication failed';
        if (typeof errData.detail === 'string') {
          errorMsg = errData.detail;
        } else if (errData.detail && typeof errData.detail === 'object') {
          errorMsg = errData.detail.message || errData.detail.error || JSON.stringify(errData.detail);
        }
        throw new Error(errorMsg);
      }

      if (authMode === 'register') {
        setAuthMode('login');
        setAuthError('Registration successful! Please login.');
      } else {
        await fetchMe();
      }
    } catch (err: any) {
      setAuthError(err.message || 'An error occurred');
    }
  };

  const handleLogout = async () => {
    try {
      await apiClient.fetch('/api/v1/auth/logout', { method: 'POST' });
    } finally {
      setCurrentUser(null);
      setFiles([]);
    }
  };

  const fetchFiles = async () => {
    try {
      const res = await apiClient.fetch('/api/v1/files');
      if (res.ok) {
        const data: FileItem[] = await res.json();
        setFiles(data);
      }
    } catch (err) {
      console.error('Error fetching files:', err);
    }
  };

  const handleFileUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedFile) return;

    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      const res = await apiClient.fetch('/api/v1/files', {
        method: 'POST',
        body: formData,
      });

      if (res.ok) {
        setSelectedFile(null);
        await fetchFiles();
      } else {
        const err = await res.json().catch(() => ({}));
        const msg = typeof err.detail === 'string' ? err.detail : (err.detail?.message || 'Upload failed');
        alert(`Upload failed: ${msg}`);
      }
    } catch (err: any) {
      alert(`Upload error: ${err.message}`);
    } finally {
      setIsUploading(false);
    }
  };

  const handleDownload = async (fileId: string, filename: string) => {
    try {
      const res = await apiClient.fetch(`/api/v1/files/${fileId}/download`);
      if (res.ok) {
        const blob = await res.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
      } else {
        alert('Download failed or permission denied.');
      }
    } catch (err: any) {
      alert(`Download error: ${err.message}`);
    }
  };

  const handleRename = async (fileId: string) => {
    if (!newFilename) return;

    try {
      const res = await apiClient.fetch(`/api/v1/files/${fileId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ new_filename: newFilename }),
      });

      if (res.ok) {
        setRenameId(null);
        setNewFilename('');
        await fetchFiles();
      }
    } catch (err: any) {
      alert(`Rename error: ${err.message}`);
    }
  };

  const handleDelete = async (fileId: string) => {
    if (!confirm('Are you sure you want to delete this file?')) return;

    try {
      const res = await apiClient.fetch(`/api/v1/files/${fileId}`, {
        method: 'DELETE',
      });

      if (res.ok) {
        await fetchFiles();
      }
    } catch (err: any) {
      alert(`Delete error: ${err.message}`);
    }
  };

  const fetchClusterHealth = async () => {
    const updated: Record<string, { health: any | null; error: string | null }> = {};

    for (const node of DEFAULT_NODES) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000);
        const response = await fetch(`http://localhost:${node.port}/api/v1/raft/cluster`, { signal: controller.signal });
        clearTimeout(timeoutId);

        if (response.ok) {
          const data = await response.json();
          updated[node.id] = { health: data, error: null };
          if (data.role === 'LEADER') {
            const leaderUrl = `http://localhost:${node.port}`;
            apiClient.setPreferredNodeUrl(leaderUrl);
          }
        } else {
          const hController = new AbortController();
          const hTimeoutId = setTimeout(() => hController.abort(), 2000);
          const hRes = await fetch(`http://localhost:${node.port}/api/v1/health`, { signal: hController.signal });
          clearTimeout(hTimeoutId);

          if (hRes.ok) {
            const hData = await hRes.json();
            updated[node.id] = { health: hData, error: null };
          } else {
            updated[node.id] = { health: null, error: `HTTP ${response.status}` };
          }
        }
      } catch (err: any) {
        updated[node.id] = { health: null, error: err.message || 'Offline' };
      }
    }

    setNodeData(updated);
    setLastUpdated(new Date().toLocaleTimeString());
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-6 md:p-10">
      <header className="max-w-6xl mx-auto mb-8 pb-6 border-b border-slate-800 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-3">
            <ServerIcon className="w-8 h-8 text-indigo-400" />
            <h1 className="text-3xl font-bold tracking-tight text-white">Secure Distributed Dropbox</h1>
          </div>
          <p className="text-slate-400 text-sm mt-1">
            Academic Operating Systems & Distributed Systems Project | Custom Raft Consensus Architecture
          </p>
        </div>

        {/* User Account Bar & Tab Navigation */}
        <div className="flex items-center gap-4">
          {/* Automatic Client Failover Badge */}
          <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-900 border border-slate-800 rounded-lg text-xs">
            <span className={`w-2.5 h-2.5 rounded-full ${connState.allNodesOffline ? 'bg-rose-500 animate-pulse' : connState.isFailover ? 'bg-amber-400' : 'bg-emerald-400'}`}></span>
            <span className="text-slate-300">Connected: <strong className="text-white">{connState.currentNodeName}</strong></span>
            {connState.isFailover && <span className="text-[10px] text-amber-400 bg-amber-400/10 px-1.5 py-0.5 rounded font-mono font-semibold">FAILOVER</span>}
          </div>

          <div className="bg-slate-900 p-1 border border-slate-800 rounded-lg flex gap-1">
            <button
              onClick={() => setActiveTab('files')}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${
                activeTab === 'files' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              My Dropbox Storage
            </button>
            <button
              onClick={() => setActiveTab('nodes')}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${
                activeTab === 'nodes' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              3-Node Cluster Topology
            </button>
          </div>

          {currentUser && (
            <div className="flex items-center gap-3 bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5">
              <div className="text-right text-xs">
                <span className="block font-semibold text-slate-200">{currentUser.username}</span>
                <span className="text-indigo-400 font-mono text-[10px] uppercase">{currentUser.role}</span>
              </div>
              <button
                onClick={handleLogout}
                className="text-xs bg-rose-500/10 hover:bg-rose-500/20 text-rose-300 border border-rose-500/30 px-2.5 py-1 rounded"
              >
                Logout
              </button>
            </div>
          )}
        </div>
      </header>

      <main className="max-w-6xl mx-auto space-y-8">
        {connState.statusMessage && (
          <div className="bg-amber-950/40 border border-amber-800/60 text-amber-200 text-xs px-4 py-2.5 rounded-lg flex items-center gap-2 shadow-lg">
            <span className="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span>
            <span>{connState.statusMessage}</span>
          </div>
        )}

        {/* TAB 1: MY DROPBOX STORAGE & AUTHENTICATION */}
        {activeTab === 'files' && (
          <div>
            {!currentUser ? (
              /* AUTHENTICATION SCREEN */
              <div className="max-w-md mx-auto bg-slate-900 border border-slate-800 rounded-2xl p-8 shadow-2xl">
                <div className="text-center mb-6">
                  <FolderIcon className="w-12 h-12 text-indigo-400 mx-auto mb-2" />
                  <h2 className="text-2xl font-bold text-white">
                    {authMode === 'login' ? 'Login to Secure Dropbox' : 'Create New Account'}
                  </h2>
                  <p className="text-xs text-slate-400 mt-1">Argon2id Hashed & HttpOnly Cookie Authenticated</p>
                </div>

                {authError && (
                  <div className="mb-4 text-xs bg-rose-950/50 border border-rose-900 text-rose-300 p-3 rounded-lg text-center">
                    {authError}
                  </div>
                )}

                <form onSubmit={handleAuthSubmit} className="space-y-4">
                  {authMode === 'register' && (
                    <div>
                      <label className="block text-xs font-medium text-slate-400 mb-1">Email Address</label>
                      <input
                        type="email"
                        required
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-indigo-500"
                        placeholder="user@domain.com"
                      />
                    </div>
                  )}

                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1">
                      {authMode === 'login' ? 'Username or Email' : 'Username'}
                    </label>
                    <input
                      type="text"
                      required
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-indigo-500"
                      placeholder="username"
                    />
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1">Password</label>
                    <input
                      type="password"
                      required
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-indigo-500"
                      placeholder="••••••••"
                    />
                  </div>

                  <button
                    type="submit"
                    className="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-medium py-2.5 rounded-lg text-sm transition-all shadow-lg shadow-indigo-950/20"
                  >
                    {authMode === 'login' ? 'Sign In' : 'Register Account'}
                  </button>
                </form>

                <div className="mt-6 text-center text-xs text-slate-400">
                  {authMode === 'login' ? (
                    <span>
                      Don't have an account?{' '}
                      <button onClick={() => setAuthMode('register')} className="text-indigo-400 hover:underline">
                        Register
                      </button>
                    </span>
                  ) : (
                    <span>
                      Already registered?{' '}
                      <button onClick={() => setAuthMode('login')} className="text-indigo-400 hover:underline">
                        Sign In
                      </button>
                    </span>
                  )}
                </div>
              </div>
            ) : (
              /* FILE MANAGEMENT DASHBOARD */
              <div className="space-y-6">
                {/* Upload Action Bar */}
                <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-6 flex flex-col md:flex-row items-center justify-between gap-4">
                  <div>
                    <h2 className="text-lg font-semibold text-white">File Manager</h2>
                    <p className="text-xs text-slate-400">Upload, download, rename, and manage files securely with SHA-256 integrity verification.</p>
                  </div>

                  <form onSubmit={handleFileUpload} className="flex items-center gap-3">
                    <input
                      type="file"
                      id="file-upload"
                      className="hidden"
                      onChange={(e) => setSelectedFile(e.target.files ? e.target.files[0] : null)}
                    />
                    <label
                      htmlFor="file-upload"
                      className="cursor-pointer bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs px-3.5 py-2 rounded-lg border border-slate-700 font-medium transition-all"
                    >
                      {selectedFile ? selectedFile.name : 'Choose File'}
                    </label>

                    <button
                      type="submit"
                      disabled={!selectedFile || isUploading}
                      className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white text-xs px-4 py-2 rounded-lg font-medium flex items-center gap-1.5 transition-all shadow"
                    >
                      <UploadIcon className="w-4 h-4" />
                      {isUploading ? 'Uploading...' : 'Upload File'}
                    </button>
                  </form>
                </div>

                {/* File Explorer Table */}
                <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
                  <table className="w-full text-left text-sm text-slate-300">
                    <thead className="bg-slate-950 text-xs uppercase font-mono text-slate-400 border-b border-slate-800">
                      <tr>
                        <th className="px-6 py-3.5">File Name</th>
                        <th className="px-6 py-3.5">Size</th>
                        <th className="px-6 py-3.5">SHA-256 Integrity</th>
                        <th className="px-6 py-3.5">Version</th>
                        <th className="px-6 py-3.5 text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60">
                      {files.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="text-center py-10 text-slate-500 italic text-sm">
                            No files uploaded yet. Choose a file above to test secure upload.
                          </td>
                        </tr>
                      ) : (
                        files.map((file) => (
                          <tr key={file.id} className="hover:bg-slate-800/40 transition-colors">
                            <td className="px-6 py-4 font-medium text-slate-100">
                              {renameId === file.id ? (
                                <div className="flex items-center gap-2">
                                  <input
                                    type="text"
                                    value={newFilename}
                                    onChange={(e) => setNewFilename(e.target.value)}
                                    className="bg-slate-950 border border-indigo-500 text-xs text-white px-2 py-1 rounded"
                                  />
                                  <button onClick={() => handleRename(file.id)} className="text-xs text-emerald-400">Save</button>
                                  <button onClick={() => setRenameId(null)} className="text-xs text-slate-400">Cancel</button>
                                </div>
                              ) : (
                                <span>{file.sanitized_name}</span>
                              )}
                            </td>
                            <td className="px-6 py-4 font-mono text-xs text-slate-400">{formatBytes(file.size_bytes)}</td>
                            <td className="px-6 py-4 font-mono text-[11px] text-indigo-300 truncate max-w-[150px]" title={file.current_sha256}>
                              {file.current_sha256.substring(0, 16)}...
                            </td>
                            <td className="px-6 py-4">
                              <span className="bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 px-2 py-0.5 rounded text-xs font-mono">
                                v{file.current_version}
                              </span>
                            </td>
                            <td className="px-6 py-4 text-right space-x-2">
                              <button
                                onClick={() => handleDownload(file.id, file.sanitized_name)}
                                className="p-1.5 hover:bg-slate-800 rounded text-slate-400 hover:text-emerald-400 transition-colors"
                                title="Download File"
                              >
                                <DownloadIcon className="w-4 h-4" />
                              </button>
                              <button
                                onClick={() => { setRenameId(file.id); setNewFilename(file.sanitized_name); }}
                                className="p-1.5 hover:bg-slate-800 rounded text-slate-400 hover:text-indigo-400 transition-colors"
                                title="Rename File"
                              >
                                <EditIcon className="w-4 h-4" />
                              </button>
                              <button
                                onClick={() => handleDelete(file.id)}
                                className="p-1.5 hover:bg-slate-800 rounded text-slate-400 hover:text-rose-400 transition-colors"
                                title="Delete File"
                              >
                                <TrashIcon className="w-4 h-4" />
                              </button>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* TAB 2: SYSTEM TOPOLOGY DASHBOARD */}
        {activeTab === 'nodes' && (
          <div className="space-y-6">
            <div className="bg-slate-900/60 border border-indigo-500/20 rounded-xl p-6 shadow-xl backdrop-blur flex items-center justify-between">
              <div>
                <div className="flex items-center gap-3 mb-1">
                  <ShieldCheckIcon className="w-6 h-6 text-indigo-400" />
                  <h2 className="text-xl font-semibold text-white">3-Node Cluster Infrastructure Topology</h2>
                </div>
                <p className="text-slate-300 text-sm">
                  Active monitoring of Node 1, Node 2, and Node 3 endpoints and peer consensus configurations.
                </p>
              </div>
              <div className="bg-slate-900 border border-slate-800 rounded-lg px-4 py-2 text-xs text-slate-400 flex items-center gap-2">
                <ActivityIcon className="w-4 h-4 text-emerald-400 animate-pulse" />
                <span>Last Polled: {lastUpdated}</span>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {DEFAULT_NODES.map((node) => {
                const info = nodeData[node.id];
                const health = info?.health;
                const error = info?.error;
                const isOnline = !!health;

                return (
                  <div
                    key={node.id}
                    className={`border rounded-xl p-6 transition-all duration-300 ${
                      isOnline
                        ? 'bg-slate-900/80 border-emerald-500/30 shadow-lg shadow-emerald-950/10'
                        : 'bg-slate-900/40 border-slate-800'
                    }`}
                  >
                    <div className="flex items-center justify-between mb-4">
                      <span className="font-mono text-sm uppercase tracking-wider text-slate-400 font-semibold">
                        {node.id}
                      </span>
                      <span
                        className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
                          isOnline
                            ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                            : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                        }`}
                      >
                        <span className={`w-2 h-2 rounded-full ${isOnline ? 'bg-emerald-400 animate-ping' : 'bg-rose-400'}`} />
                        {isOnline ? 'ONLINE' : 'OFFLINE'}
                      </span>
                    </div>

                    <div className="space-y-3 text-sm">
                      <div className="flex justify-between py-1 border-b border-slate-800/60">
                        <span className="text-slate-400">HTTP Endpoint</span>
                        <span className="font-mono text-indigo-300">:{node.port}</span>
                      </div>
                      <div className="flex justify-between py-1 border-b border-slate-800/60">
                        <span className="text-slate-400">gRPC Consensus</span>
                        <span className="font-mono text-indigo-300">:{node.grpcPort}</span>
                      </div>
                      <div className="flex justify-between py-1 border-b border-slate-800/60">
                        <span className="text-slate-400">Phase</span>
                        <span className="text-indigo-400 font-semibold">{health?.phase || 'phase-1'}</span>
                      </div>
                      <div className="pt-2">
                        <span className="text-xs text-slate-500 uppercase font-mono tracking-wider block mb-1">Configured Peers</span>
                        <div className="flex flex-wrap gap-1">
                          {health?.peers && health.peers.length > 0 ? (
                            health.peers.map((peer: string, idx: number) => (
                              <span key={idx} className="text-xs bg-slate-800 text-slate-300 font-mono px-2 py-0.5 rounded">
                                {peer}
                              </span>
                            ))
                          ) : (
                            <span className="text-xs text-slate-500 italic">None or Offline</span>
                          )}
                        </div>
                      </div>
                      {error && (
                        <div className="mt-3 text-xs bg-rose-950/40 text-rose-300 p-2 rounded border border-rose-900/50">
                          Status Error: {error}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
