import { useState, FormEvent } from 'react';
import { ArrowLeft, Loader2, Sparkles } from 'lucide-react';
import { useAuthStore } from '../store/useAuthStore';
import { signIn, signUp } from '../services/authApi';

type LoginPageProps = {
  onBackToLanding?: () => void;
};

const LoginPage = ({ onBackToLanding }: LoginPageProps) => {
  const [isSignUp, setIsSignUp] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [rememberMe, setRememberMe] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const setAuth = useAuthStore((s) => s.setAuth);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setInfo(null);
    setSubmitting(true);
    try {
      if (isSignUp) {
        const result = await signUp({
          email,
          password,
          full_name: fullName.trim() || undefined,
        });
        if (result.kind === 'session') {
          setAuth(result.data.access_token, result.data.user, result.data.refresh_token);
        } else {
          setInfo(result.message);
        }
      } else {
        const res = await signIn({ email, password });
        setAuth(res.access_token, res.user, res.refresh_token);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Request failed');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="login-page">
      {/* Background decorative elements */}
      <div className="login-bg-glow login-bg-glow--top" />
      <div className="login-bg-glow login-bg-glow--bottom" />
      <div className="login-sparkle">
        <Sparkles size={48} strokeWidth={1} />
      </div>

      {/* Card */}
      <div className="login-card">
        {onBackToLanding && (
          <button
            type="button"
            onClick={onBackToLanding}
            className="mb-6 flex w-full cursor-pointer items-center justify-center gap-2 border-none bg-transparent text-sm font-medium text-[var(--text-muted)] hover:text-[var(--text-secondary)]"
          >
            <ArrowLeft className="h-4 w-4 shrink-0" aria-hidden />
            Back to Purely BI
          </button>
        )}
        <div className="login-brand">
          <div className="login-logo-icon">
            <Sparkles size={18} />
          </div>
          <span className="login-brand-text">Purely BI</span>
        </div>

        <h1 className="login-heading">
          {isSignUp ? 'Create account' : 'Welcome Back'}
        </h1>

        <form onSubmit={handleSubmit} className="login-form">
          {error && (
            <div className="login-error">{error}</div>
          )}
          {info && (
            <div className="login-error" style={{ background: 'rgba(34,197,94,0.12)', color: 'var(--login-text, #166534)', borderColor: 'rgba(34,197,94,0.35)' }}>
              {info}
            </div>
          )}

          {isSignUp && (
            <>
              <label className="login-label">Full name (optional)</label>
              <input
                type="text"
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                autoComplete="name"
                className="login-input"
                placeholder="Jane Doe"
              />
            </>
          )}

          <label className="login-label">Email Address</label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoComplete="email"
            className="login-input"
            placeholder="name@biagent.app"
          />

          <label className="login-label">Password {isSignUp && '(8+ characters)'}</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            minLength={isSignUp ? 8 : 1}
            autoComplete={isSignUp ? 'new-password' : 'current-password'}
            className="login-input"
            placeholder="Password ••••••••"
          />

          <div className="login-options">
            <label className="login-remember">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={(e) => setRememberMe(e.target.checked)}
                className="login-checkbox"
              />
              <span>Remember Me</span>
            </label>
            <button type="button" className="login-link">
              Forgot Password?
            </button>
          </div>

          <button
            type="submit"
            disabled={submitting}
            className="login-btn"
          >
            {submitting ? (
              <>
                <Loader2 size={18} className="animate-spin" />
                {isSignUp ? 'Creating account…' : 'Signing in…'}
              </>
            ) : (
              isSignUp ? 'Create account' : 'Sign In'
            )}
          </button>

          <p className="login-footer-text">
            {isSignUp ? (
              <>
                Already have an account?{' '}
                <button
                  type="button"
                  className="login-link"
                  onClick={() => {
                    setIsSignUp(false);
                    setError(null);
                    setInfo(null);
                  }}
                >
                  Sign in
                </button>
              </>
            ) : (
              <>
                Don&apos;t have an account?{' '}
                <button
                  type="button"
                  className="login-link"
                  onClick={() => {
                    setIsSignUp(true);
                    setError(null);
                    setInfo(null);
                  }}
                >
                  Create one
                </button>
              </>
            )}
          </p>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;
