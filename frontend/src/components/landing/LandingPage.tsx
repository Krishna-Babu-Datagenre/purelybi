/**
 * Purely BI — marketing landing (client-only: motion + scroll).
 */
import { useRef, type ReactNode } from 'react';
import {
  MotionConfig,
  motion,
  useScroll,
  useTransform,
  useReducedMotion,
} from 'motion/react';
import {
  ArrowRight,
  BarChart3,
  Bell,
  LayoutDashboard,
  MessageSquare,
  Plug,
  Sparkles,
  Play,
  Headphones,
  Check,
  Zap,
} from 'lucide-react';

type LandingPageProps = {
  onOpenAuth: () => void;
};

const navLink = 'text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] cursor-pointer';

const Section = ({
  id,
  className = '',
  children,
}: {
  id?: string;
  className?: string;
  children: ReactNode;
}) => (
  <section id={id} className={`relative scroll-mt-24 ${className}`}>
    {children}
  </section>
);

const FadeIn = ({
  children,
  className = '',
  delay = 0,
}: {
  children: ReactNode;
  className?: string;
  delay?: number;
}) => {
  const reduce = useReducedMotion();
  return (
    <motion.div
      className={className}
      initial={reduce ? { opacity: 1, y: 0 } : { opacity: 0, y: 28 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: '-80px' }}
      transition={
        reduce ? { duration: 0 } : { duration: 0.55, ease: [0.22, 1, 0.36, 1], delay }
      }
    >
      {children}
    </motion.div>
  );
};

function HeroParallax({ onOpenAuth, onWatchDemo }: { onOpenAuth: () => void; onWatchDemo: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  const reduce = useReducedMotion();
  const { scrollYProgress } = useScroll({
    target: ref,
    offset: ['start start', 'end start'],
  });
  const yBg = useTransform(scrollYProgress, [0, 1], reduce ? [0, 0] : [0, 120]);
  const yGrid = useTransform(scrollYProgress, [0, 1], reduce ? [0, 0] : [0, 60]);
  const opacity = useTransform(scrollYProgress, [0, 0.45], [1, 0.35]);

  return (
    <div ref={ref} className="relative min-h-[min(92vh,880px)] flex flex-col overflow-hidden">
      {/* Parallax layers */}
      <motion.div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{ opacity }}
      >
        <motion.div
          className="absolute -top-32 left-1/2 h-[520px] w-[min(1200px,140vw)] -translate-x-1/2 rounded-full blur-3xl"
          style={{
            y: yBg,
            background:
              'radial-gradient(ellipse at center, rgba(139, 92, 246, 0.22) 0%, transparent 65%)',
          }}
        />
        <motion.div
          className="absolute -bottom-40 right-[-10%] h-[420px] w-[min(900px,90vw)] rounded-full blur-3xl"
          style={{
            y: yBg,
            background:
              'radial-gradient(ellipse at center, rgba(6, 182, 212, 0.12) 0%, transparent 70%)',
          }}
        />
        <motion.div
          className="absolute inset-0 opacity-[0.35]"
          style={{
            y: yGrid,
            backgroundImage:
              'linear-gradient(rgba(139, 92, 246, 0.06) 1px, transparent 1px), linear-gradient(90deg, rgba(139, 92, 246, 0.06) 1px, transparent 1px)',
            backgroundSize: '64px 64px',
            maskImage: 'radial-gradient(ellipse at center, black 0%, transparent 75%)',
          }}
        />
      </motion.div>

      <div className="relative z-10 mx-auto flex w-full max-w-6xl flex-1 flex-col justify-center px-4 pb-20 pt-28 sm:px-6 lg:px-8 lg:pt-32">
        <motion.div
          initial={reduce ? { opacity: 1, y: 0 } : { opacity: 0, y: 24 }}
          animate={{ opacity: 1, y: 0 }}
          transition={reduce ? { duration: 0 } : { duration: 0.55, ease: [0.22, 1, 0.36, 1] }}
          className="max-w-3xl"
        >
          <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-[var(--border-default)] bg-[var(--bg-surface)]/80 px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] backdrop-blur-md">
            <Sparkles className="h-3.5 w-3.5 text-[var(--brand)]" aria-hidden />
            <span>Natural language BI for modern teams</span>
          </div>
          <h1 className="text-4xl font-bold tracking-tight text-[var(--text-primary)] sm:text-5xl lg:text-6xl lg:leading-[1.05]">
            Understand your data —{' '}
            <span className="bg-gradient-to-r from-[#06b6d4] via-[#8B5CF6] to-[#A78BFA] bg-clip-text text-transparent">
              effortlessly
            </span>
            .
          </h1>
          <p className="mt-6 max-w-xl text-lg leading-relaxed text-[var(--text-secondary)]">
            Purely BI connects your tools, syncs what matters, and lets you ask questions in plain English. Build
            live reports, dashboards, and alerts without wrestling with SQL or endless exports.
          </p>
          <div className="mt-10 flex flex-wrap items-center gap-4">
            <button
              type="button"
              onClick={onOpenAuth}
              className="group inline-flex cursor-pointer items-center gap-2 rounded-full bg-gradient-to-r from-[#06b6d4] via-[#8B5CF6] to-[#A78BFA] px-7 py-3.5 text-[15px] font-semibold text-white shadow-[0_8px_32px_rgba(139,92,246,0.35)] hover:shadow-[0_12px_40px_rgba(139,92,246,0.45)]"
            >
              Get started
              <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5" aria-hidden />
            </button>
            <button
              type="button"
              onClick={onWatchDemo}
              className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-[var(--border-default)] bg-[var(--bg-surface)]/60 px-6 py-3.5 text-[15px] font-semibold text-[var(--text-primary)] backdrop-blur-md hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)]/80"
            >
              <Play className="h-4 w-4 fill-current" aria-hidden />
              Watch demo
            </button>
          </div>
          <p className="mt-8 text-sm text-[var(--text-muted)]">
            Why Purely BI? One place for connections, questions, and reporting — with AI that speaks your language,
            not spreadsheets.
          </p>
        </motion.div>

        {/* Floating preview card */}
        <motion.div
          initial={reduce ? { opacity: 1, y: 0 } : { opacity: 0, y: 40 }}
          animate={{ opacity: 1, y: 0 }}
          transition={reduce ? { duration: 0 } : { duration: 0.65, delay: 0.12, ease: [0.22, 1, 0.36, 1] }}
          className="pointer-events-none relative mt-16 hidden select-none lg:block"
        >
          <div className="relative mx-auto max-w-4xl rounded-2xl border border-[var(--border-default)] bg-gradient-to-br from-[var(--bg-surface)] to-[#0d0d14] p-1 shadow-[0_24px_80px_rgba(0,0,0,0.45)]">
            <div className="flex items-center gap-2 rounded-t-xl border-b border-[var(--border-subtle)] bg-[var(--bg-elevated)]/80 px-4 py-3">
              <span className="h-2.5 w-2.5 rounded-full bg-red-500/80" />
              <span className="h-2.5 w-2.5 rounded-full bg-amber-400/80" />
              <span className="h-2.5 w-2.5 rounded-full bg-emerald-500/80" />
              <span className="ml-3 text-xs text-[var(--text-muted)]">app.purelybi.com — Executive overview</span>
            </div>
            <div className="grid gap-4 p-6 sm:grid-cols-3">
              {[
                { label: 'Revenue (MTD)', value: '$2.4M', delta: '+12.4%' },
                { label: 'Active users', value: '18.2k', delta: '+3.1%' },
                { label: 'Sync health', value: '99.2%', delta: 'Stable' },
              ].map((k) => (
                <div
                  key={k.label}
                  className="rounded-xl border border-[var(--border-subtle)] bg-[var(--bg-canvas)]/80 p-4"
                >
                  <p className="text-xs text-[var(--text-muted)]">{k.label}</p>
                  <p className="mt-2 text-2xl font-semibold tracking-tight text-[var(--text-primary)]">{k.value}</p>
                  <p className="mt-1 text-xs font-medium text-emerald-400/90">{k.delta}</p>
                </div>
              ))}
            </div>
            <div className="border-t border-[var(--border-subtle)] px-6 py-4">
              <div className="flex items-start gap-3 rounded-xl bg-[var(--brand-dim)]/50 p-3">
                <MessageSquare className="mt-0.5 h-5 w-5 shrink-0 text-[var(--brand)]" aria-hidden />
                <div>
                  <p className="text-xs font-medium text-[var(--text-secondary)]">Ask Purely BI</p>
                  <p className="text-sm text-[var(--text-primary)]">
                    &ldquo;Show me revenue by region last quarter, and flag any segment under target.&rdquo;
                  </p>
                </div>
              </div>
            </div>
          </div>
        </motion.div>
      </div>
    </div>
  );
}

const features = [
  {
    icon: Plug,
    title: '500+ connectors',
    body: 'Connect warehouses, SaaS, ads, and spreadsheets — then keep them in sync automatically.',
  },
  {
    icon: MessageSquare,
    title: 'Natural language setup',
    body: 'Describe what you want connected; Purely BI maps the right sources and schedules.',
  },
  {
    icon: BarChart3,
    title: 'Ask questions in plain English',
    body: 'Explore metrics, drill into segments, and get explanations you can share with stakeholders.',
  },
  {
    icon: LayoutDashboard,
    title: 'Dynamic reports & dashboards',
    body: 'Compose mixed visuals, KPI blocks, and layouts that refresh as your data updates.',
  },
  {
    icon: Bell,
    title: 'Scheduled alerts',
    body: 'Get notified when thresholds break or trends shift — before they become surprises.',
  },
  {
    icon: Zap,
    title: 'Built for speed',
    body: 'A fast, focused UI that keeps you in flow from first connection to final export.',
  },
];

const pricingTiers = [
  {
    name: 'Starter',
    price: '$29',
    period: '/month',
    blurb: 'For individuals and small teams getting started with unified insights.',
    features: ['Up to 3 data sources', 'Natural language Q&A', '5 scheduled reports', 'Email support'],
    cta: 'Start free trial',
    highlighted: false,
  },
  {
    name: 'Growth',
    price: '$99',
    period: '/month',
    blurb: 'For growing teams that need scale, alerts, and shared workspaces.',
    features: [
      'Unlimited sources',
      'Advanced NL + report builder',
      'Scheduled alerts & webhooks',
      'Priority chat support',
      'SSO (SAML)',
    ],
    cta: 'Get started',
    highlighted: true,
  },
  {
    name: 'Enterprise',
    price: 'Custom',
    period: '',
    blurb: 'Security, governance, and dedicated support for organization-wide data programs.',
    features: ['VPC & data residency options', 'Dedicated CSM', 'SLA & audit support', 'Custom connectors'],
    cta: 'Talk to sales',
    highlighted: false,
  },
];

export default function LandingPage({ onOpenAuth }: LandingPageProps) {
  const scrollToDemo = () => {
    document.getElementById('demo')?.scrollIntoView({ behavior: 'smooth' });
  };

  return (
    <MotionConfig reducedMotion="user">
      <div className="min-h-screen bg-[var(--bg-canvas)] text-[var(--text-primary)]">
        {/* Top nav */}
        <header className="fixed left-0 right-0 top-0 z-50 border-b border-[var(--border-subtle)] bg-[var(--bg-canvas)]/75 backdrop-blur-xl">
          <div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-4 sm:px-6 lg:px-8">
            <a href="#top" className="flex cursor-pointer items-center gap-2 rounded-lg outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]">
              <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-[#06b6d4] to-[#8B5CF6] text-white shadow-lg shadow-[var(--brand-glow)]">
                <Sparkles className="h-4 w-4" aria-hidden />
              </span>
              <span className="text-sm font-semibold tracking-tight">Purely BI</span>
            </a>
            <nav className="hidden items-center gap-8 md:flex" aria-label="Primary">
              <button type="button" className={navLink} onClick={() => document.getElementById('overview')?.scrollIntoView({ behavior: 'smooth' })}>
                Product
              </button>
              <button type="button" className={navLink} onClick={scrollToDemo}>
                Demo
              </button>
              <button type="button" className={navLink} onClick={() => document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' })}>
                Features
              </button>
              <button type="button" className={navLink} onClick={() => document.getElementById('pricing')?.scrollIntoView({ behavior: 'smooth' })}>
                Pricing
              </button>
              <button type="button" className={navLink} onClick={() => document.getElementById('support')?.scrollIntoView({ behavior: 'smooth' })}>
                Support
              </button>
            </nav>
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={onOpenAuth}
                className="cursor-pointer rounded-full px-4 py-2 text-sm font-semibold text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
              >
                Log in
              </button>
              <button
                type="button"
                onClick={onOpenAuth}
                className="cursor-pointer rounded-full bg-gradient-to-r from-[#06b6d4] to-[#8B5CF6] px-4 py-2 text-sm font-semibold text-white shadow-md shadow-[var(--brand-glow)] hover:opacity-95"
              >
                Get started
              </button>
            </div>
          </div>
        </header>

        <main id="top">
          <HeroParallax onOpenAuth={onOpenAuth} onWatchDemo={scrollToDemo} />

          {/* Product overview */}
          <Section id="overview" className="border-t border-[var(--border-subtle)] bg-[var(--bg-canvas)] py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <FadeIn>
                <p className="text-center text-xs font-semibold uppercase tracking-[0.2em] text-[var(--brand)]">
                  Product overview
                </p>
                <h2 className="mx-auto mt-3 max-w-2xl text-center text-3xl font-bold tracking-tight sm:text-4xl">
                  One platform to connect, ask, and act on your data
                </h2>
                <p className="mx-auto mt-5 max-w-2xl text-center text-lg text-[var(--text-secondary)]">
                  Purely BI brings together integrations, sync, and AI so you spend less time preparing data and
                  more time making decisions. Outcomes: faster insights, clearer reporting, and fewer manual
                  handoffs between tools.
                </p>
              </FadeIn>
              <div className="mt-10 grid gap-4 sm:grid-cols-3">
                {[
                  { t: 'Better decisions', d: 'See the full story across sources with consistent metrics.' },
                  { t: 'Faster answers', d: 'Ask in natural language and iterate without filing tickets.' },
                  { t: 'Less busywork', d: 'Automate sync, refresh, and alerts so teams stay in sync.' },
                ].map((item, i) => (
                  <FadeIn key={item.t} delay={i * 0.08} className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)]/80 p-6 backdrop-blur-sm">
                    <h3 className="text-lg font-semibold text-[var(--text-primary)]">{item.t}</h3>
                    <p className="mt-2 text-sm leading-relaxed text-[var(--text-secondary)]">{item.d}</p>
                  </FadeIn>
                ))}
              </div>
            </div>
          </Section>

          {/* Demo */}
          <Section id="demo" className="border-t border-[var(--border-subtle)] py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <FadeIn>
                <p className="text-center text-xs font-semibold uppercase tracking-[0.2em] text-[var(--brand)]">
                  See it in action
                </p>
                <h2 className="mx-auto mt-3 max-w-2xl text-center text-3xl font-bold tracking-tight sm:text-4xl">
                  A guided walkthrough of the product
                </h2>
                <p className="mx-auto mt-5 max-w-2xl text-center text-lg text-[var(--text-secondary)]">
                  Watch how teams connect sources, ask questions, and ship reports — without leaving the workspace.
                </p>
              </FadeIn>
              <FadeIn delay={0.1} className="mt-12">
                <div className="relative z-0 overflow-hidden rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
                  <div className="aspect-video w-full">
                    <iframe
                      className="h-full w-full"
                      src="https://www.youtube-nocookie.com/embed/TmhJJiVrlU8"
                      title="Purely BI product demo video"
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                      allowFullScreen
                    />
                  </div>
                </div>
                <p className="mt-6 text-center text-sm text-[var(--text-muted)]">
                  Swap this embed for your own product walkthrough when you are ready to publish.
                </p>
              </FadeIn>
            </div>
          </Section>

          {/* Features */}
          <Section id="features" className="border-t border-[var(--border-subtle)] bg-[linear-gradient(180deg,var(--bg-canvas)_0%,#0a0a12_100%)] py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <FadeIn>
                <p className="text-center text-xs font-semibold uppercase tracking-[0.2em] text-[var(--brand)]">
                  Capabilities
                </p>
                <h2 className="mx-auto mt-3 max-w-2xl text-center text-3xl font-bold tracking-tight sm:text-4xl">
                  Everything you need to go from raw data to confident decisions
                </h2>
              </FadeIn>
              <div className="mt-10 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                {features.map((f, i) => (
                  <FadeIn key={f.title} delay={i * 0.05}>
                    <motion.div
                      className="group h-full rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)]/70 p-6 backdrop-blur-sm cursor-pointer"
                      whileHover={{ y: -4 }}
                      transition={{ type: 'spring', stiffness: 400, damping: 28 }}
                    >
                      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-[var(--brand-dim)] text-[var(--brand)]">
                        <f.icon className="h-5 w-5" aria-hidden />
                      </div>
                      <h3 className="mt-4 text-lg font-semibold text-[var(--text-primary)]">{f.title}</h3>
                      <p className="mt-2 text-sm leading-relaxed text-[var(--text-secondary)]">{f.body}</p>
                    </motion.div>
                  </FadeIn>
                ))}
              </div>
            </div>
          </Section>

          {/* Pricing */}
          <Section id="pricing" className="border-t border-[var(--border-subtle)] py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <FadeIn>
                <p className="text-center text-xs font-semibold uppercase tracking-[0.2em] text-[var(--brand)]">
                  Pricing
                </p>
                <h2 className="mx-auto mt-3 max-w-2xl text-center text-3xl font-bold tracking-tight sm:text-4xl">
                  Simple tiers that scale with you
                </h2>
                <p className="mx-auto mt-5 max-w-2xl text-center text-lg text-[var(--text-secondary)]">
                  Transparent pricing with room to grow. Enterprise includes concierge onboarding and custom
                  integrations.
                </p>
              </FadeIn>
              <div className="mt-10 grid gap-4 lg:grid-cols-3">
                {pricingTiers.map((tier, i) => (
                  <FadeIn key={tier.name} delay={i * 0.06}>
                    <div
                      className={`relative flex h-full flex-col rounded-2xl border p-6 backdrop-blur-sm ${
                        tier.highlighted
                          ? 'border-[var(--brand)] bg-[radial-gradient(ellipse_at_top,rgba(139,92,246,0.18),transparent_55%),var(--bg-surface)] shadow-[0_0_0_1px_rgba(139,92,246,0.25)]'
                          : 'border-[var(--border-default)] bg-[var(--bg-surface)]/80'
                      }`}
                    >
                      {tier.highlighted && (
                        <span className="absolute -top-3 left-1/2 -translate-x-1/2 rounded-full bg-gradient-to-r from-[#06b6d4] to-[#8B5CF6] px-3 py-1 text-[11px] font-semibold uppercase tracking-wide text-white">
                          Most popular
                        </span>
                      )}
                      <h3 className="text-lg font-semibold text-[var(--text-primary)]">{tier.name}</h3>
                      <p className="mt-2 min-h-[40px] text-sm text-[var(--text-secondary)]">{tier.blurb}</p>
                      <div className="mt-6 flex items-baseline gap-1">
                        <span className="text-4xl font-bold tracking-tight text-[var(--text-primary)]">{tier.price}</span>
                        {tier.period ? (
                          <span className="text-sm text-[var(--text-muted)]">{tier.period}</span>
                        ) : null}
                      </div>
                      <ul className="mt-6 flex flex-1 flex-col gap-3">
                        {tier.features.map((line) => (
                          <li key={line} className="flex items-start gap-2 text-sm text-[var(--text-secondary)]">
                            <Check className="mt-0.5 h-4 w-4 shrink-0 text-emerald-400" aria-hidden />
                            <span>{line}</span>
                          </li>
                        ))}
                      </ul>
                      <button
                        type="button"
                        onClick={onOpenAuth}
                        className={`mt-8 w-full cursor-pointer rounded-full py-3 text-sm font-semibold ${
                          tier.highlighted
                            ? 'bg-gradient-to-r from-[#06b6d4] to-[#8B5CF6] text-white shadow-lg shadow-[var(--brand-glow)]'
                            : 'border border-[var(--border-default)] bg-[var(--bg-canvas)] text-[var(--text-primary)] hover:border-[var(--border-strong)]'
                        }`}
                      >
                        {tier.cta}
                      </button>
                    </div>
                  </FadeIn>
                ))}
              </div>
            </div>
          </Section>

          {/* Support / concierge */}
          <Section id="support" className="border-t border-[var(--border-subtle)] bg-[var(--bg-surface)]/40 py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <div className="grid gap-10 lg:grid-cols-2 lg:items-center">
                <FadeIn>
                  <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--brand)]">
                    Concierge setup
                  </p>
                  <h2 className="mt-3 text-3xl font-bold tracking-tight sm:text-4xl">
                    We help you ship your first dashboards — fast
                  </h2>
                  <p className="mt-5 text-lg text-[var(--text-secondary)]">
                    Our team can assist with onboarding, modeling priorities, and rollout best practices. Whether
                    you are migrating from spreadsheets or consolidating BI tools, we will meet you where you are.
                  </p>
                  <button
                    type="button"
                    onClick={onOpenAuth}
                    className="mt-8 inline-flex cursor-pointer items-center gap-2 rounded-full border border-[var(--border-default)] bg-[var(--bg-canvas)] px-6 py-3.5 text-sm font-semibold text-[var(--text-primary)] hover:border-[var(--border-strong)]"
                  >
                    <Headphones className="h-4 w-4 text-[var(--brand)]" aria-hidden />
                    Get in touch
                    <ArrowRight className="h-4 w-4" aria-hidden />
                  </button>
                </FadeIn>
                <FadeIn delay={0.1}>
                  <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-canvas)] px-6 py-8">
                    <ul className="space-y-4 text-sm text-[var(--text-secondary)]">
                      <li className="flex gap-3">
                        <Check className="mt-0.5 h-4 w-4 shrink-0 text-[var(--brand)]" aria-hidden />
                        <span>Guided connection of priority sources and validation checks</span>
                      </li>
                      <li className="flex gap-3">
                        <Check className="mt-0.5 h-4 w-4 shrink-0 text-[var(--brand)]" aria-hidden />
                        <span>Workspace templates for exec, revenue, and ops views</span>
                      </li>
                      <li className="flex gap-3">
                        <Check className="mt-0.5 h-4 w-4 shrink-0 text-[var(--brand)]" aria-hidden />
                        <span>Office hours for teams adopting natural-language reporting</span>
                      </li>
                    </ul>
                  </div>
                </FadeIn>
              </div>
            </div>
          </Section>

          {/* Final CTA */}
          <Section className="border-t border-[var(--border-subtle)] py-24">
            <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8">
              <FadeIn>
                <div className="relative overflow-hidden rounded-3xl border border-[var(--border-default)] bg-gradient-to-br from-[#12121c] via-[#0e0e18] to-[#0a0a0f] px-8 py-16 text-center shadow-[0_32px_120px_rgba(139,92,246,0.12)] sm:px-6 lg:px-8">
                  <div
                    aria-hidden
                    className="pointer-events-none absolute inset-0 opacity-40"
                    style={{
                      backgroundImage:
                        'radial-gradient(ellipse 80% 50% at 50% -20%, rgba(139, 92, 246, 0.35) 0%, transparent 55%)',
                    }}
                  />
                  <div className="relative z-10">
                    <h2 className="text-3xl font-bold tracking-tight sm:text-4xl">
                      Ready to understand your data — effortlessly?
                    </h2>
                    <p className="mx-auto mt-4 max-w-xl text-lg text-[var(--text-secondary)]">
                      Join teams who replaced static exports with live, AI-ready reporting. Start in minutes.
                    </p>
                    <div className="mt-10 flex flex-wrap items-center justify-center gap-4">
                      <button
                        type="button"
                        onClick={onOpenAuth}
                        className="inline-flex cursor-pointer items-center gap-2 rounded-full bg-gradient-to-r from-[#06b6d4] via-[#8B5CF6] to-[#A78BFA] px-8 py-3.5 text-[15px] font-semibold text-white shadow-[0_8px_32px_rgba(139,92,246,0.35)]"
                      >
                        Try it now
                        <ArrowRight className="h-4 w-4" aria-hidden />
                      </button>
                      <button
                        type="button"
                        onClick={onOpenAuth}
                        className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-[var(--border-default)] bg-[var(--bg-surface)]/80 px-8 py-3.5 text-[15px] font-semibold text-[var(--text-primary)] backdrop-blur-md hover:border-[var(--border-strong)]"
                      >
                        Log in
                      </button>
                    </div>
                  </div>
                </div>
              </FadeIn>
            </div>
          </Section>

          <footer className="border-t border-[var(--border-subtle)] py-10 text-center text-sm text-[var(--text-muted)]">
            <p className="font-medium text-[var(--text-secondary)]">Purely BI</p>
            <p className="mt-2">© {new Date().getFullYear()} Purely BI. All rights reserved.</p>
          </footer>
        </main>
      </div>
    </MotionConfig>
  );
}
