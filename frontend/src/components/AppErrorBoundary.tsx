import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

/**
 * Catches render errors in the tree below and shows a recovery UI instead of a blank screen.
 */
export default class AppErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error('AppErrorBoundary:', error, info.componentStack);
  }

  render(): ReactNode {
    const { hasError, error } = this.state;
    if (hasError && error) {
      return (
        <div className="min-h-screen flex flex-col items-center justify-center gap-4 px-6 py-12 bg-[var(--bg-canvas)] text-[var(--text-primary)]">
          <h1 className="text-lg font-semibold tracking-tight">Something went wrong</h1>
          <p className="text-sm text-[var(--text-secondary)] text-center max-w-md leading-relaxed">
            The app hit an unexpected error. You can try reloading the page. If this keeps happening,
            note what you were doing and share it with support.
          </p>
          <pre className="text-xs font-mono text-[var(--text-muted)] max-w-full overflow-x-auto rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface)] p-3">
            {error.message}
          </pre>
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="rounded-xl bg-[var(--brand)] text-white font-medium px-5 py-2.5 text-sm hover:opacity-95 transition-opacity cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
          >
            Reload page
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
