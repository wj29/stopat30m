import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import type { ReactNode } from 'react';

export default function Shell({ children }: { children?: ReactNode }) {
  return (
    <div className="flex h-screen bg-gray-100">
      <Sidebar />
      <main className="flex-1 overflow-y-auto p-6">
        {children ?? <Outlet />}
      </main>
    </div>
  );
}
