import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import { useState, type ReactNode } from 'react';

export default function Shell({ children }: { children?: ReactNode }) {
  const [menuOpen, setMenuOpen] = useState(false);
  return (
    <div className="flex h-dvh min-h-0 bg-gray-50">
      <Sidebar open={menuOpen} onClose={() => setMenuOpen(false)} />
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-gray-200 bg-white px-4 md:hidden">
          <button onClick={() => setMenuOpen(true)} aria-label="打开菜单" className="rounded border border-gray-200 px-2 py-1 text-gray-600">☰</button>
          <span className="text-sm font-semibold text-gray-800">stopat30m</span>
        </header>
        <main className="min-w-0 flex-1 overflow-y-auto p-4 sm:p-6 lg:p-8">
          <div className="mx-auto w-full max-w-[1440px]">{children ?? <Outlet />}</div>
        </main>
      </div>
    </div>
  );
}
