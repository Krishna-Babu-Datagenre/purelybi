import type { ReactNode } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const baseComponents = {
  p: ({ children }: { children?: ReactNode }) => (
    <p className="mb-2 last:mb-0 leading-relaxed">{children}</p>
  ),
  ul: ({ children }: { children?: ReactNode }) => (
    <ul className="list-disc pl-5 mb-2 space-y-1">{children}</ul>
  ),
  ol: ({ children }: { children?: ReactNode }) => (
    <ol className="list-decimal pl-5 mb-2 space-y-1">{children}</ol>
  ),
  li: ({ children }: { children?: ReactNode }) => <li className="leading-relaxed">{children}</li>,
  strong: ({ children }: { children?: ReactNode }) => (
    <strong className="font-semibold text-[var(--text-primary)]">{children}</strong>
  ),
  em: ({ children }: { children?: ReactNode }) => <em className="italic">{children}</em>,
  a: ({ href, children }: { href?: string; children?: ReactNode }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-[var(--brand)] underline underline-offset-2 hover:opacity-90"
    >
      {children}
    </a>
  ),
  code: ({ className, children }: { className?: string; children?: ReactNode }) => {
    const isBlock = className?.includes('language-');
    if (isBlock) {
      return (
        <code className="block rounded-lg border border-[var(--border-subtle)] bg-[var(--bg-canvas)] p-3 text-xs font-mono overflow-x-auto my-2">
          {children}
        </code>
      );
    }
    return (
      <code className="rounded px-1.5 py-0.5 text-[0.85em] font-mono bg-[var(--bg-canvas)] border border-[var(--border-subtle)]">
        {children}
      </code>
    );
  },
  pre: ({ children }: { children?: ReactNode }) => (
    <pre className="overflow-x-auto my-2 text-[var(--text-secondary)]">{children}</pre>
  ),
  h1: ({ children }: { children?: ReactNode }) => (
    <h1 className="text-lg font-semibold mt-2 mb-1">{children}</h1>
  ),
  h2: ({ children }: { children?: ReactNode }) => (
    <h2 className="text-base font-semibold mt-2 mb-1">{children}</h2>
  ),
  h3: ({ children }: { children?: ReactNode }) => (
    <h3 className="text-sm font-semibold mt-2 mb-1">{children}</h3>
  ),
  blockquote: ({ children }: { children?: ReactNode }) => (
    <blockquote className="border-l-2 border-[var(--brand)] pl-3 my-2 text-[var(--text-secondary)] italic">
      {children}
    </blockquote>
  ),
};

interface MarkdownMessageProps {
  content: string;
  className?: string;
}

export default function MarkdownMessage({ content, className = '' }: MarkdownMessageProps) {
  return (
    <div className={`text-sm text-[var(--text-primary)] [&_*:first-child]:mt-0 ${className}`}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={baseComponents}>
        {content}
      </ReactMarkdown>
    </div>
  );
}
