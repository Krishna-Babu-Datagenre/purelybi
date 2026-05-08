import { useState, useEffect, useCallback } from 'react';
import { useAuthStore } from '../store/useAuthStore';
import { updateProfile } from '../services/authApi';
import { Save, Check, AlertCircle, User, Mail, Calendar, Loader2 } from 'lucide-react';

interface ProfilePageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

type Feedback = null | { kind: 'success'; message: string } | { kind: 'error'; message: string };

const ProfilePage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: ProfilePageProps) => {
  const user = useAuthStore((s) => s.user);
  const accessToken = useAuthStore((s) => s.accessToken);
  const validateStoredToken = useAuthStore((s) => s.validateStoredToken);

  const [fullName, setFullName] = useState(user?.full_name ?? '');
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<Feedback>(null);

  // Sync local state when user profile changes (e.g. after save)
  useEffect(() => {
    setFullName(user?.full_name ?? '');
  }, [user?.full_name]);

  const isDirty = fullName.trim() !== (user?.full_name ?? '').trim();

  const handleSave = useCallback(async () => {
    if (!accessToken || !isDirty || saving) return;
    const trimmed = fullName.trim();
    if (!trimmed) {
      setFeedback({ kind: 'error', message: 'Name cannot be empty.' });
      return;
    }

    setSaving(true);
    setFeedback(null);
    try {
      await updateProfile(accessToken, { full_name: trimmed });
      // Refresh the auth store with the latest profile data
      await validateStoredToken(true);
      setFeedback({ kind: 'success', message: 'Profile updated successfully.' });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to update profile.';
      setFeedback({ kind: 'error', message: msg });
    } finally {
      setSaving(false);
    }
  }, [accessToken, fullName, isDirty, saving, validateStoredToken]);

  // Auto-dismiss success feedback
  useEffect(() => {
    if (feedback?.kind !== 'success') return;
    const timer = setTimeout(() => setFeedback(null), 4000);
    return () => clearTimeout(timer);
  }, [feedback]);

  const initials = (user?.full_name || user?.email || 'U').slice(0, 2).toUpperCase();
  const memberSinceDate = user?.created_at
    ? new Date(user.created_at)
    : user?.trial_ends_at
      ? new Date(new Date(user.trial_ends_at).getTime() - 7 * 24 * 60 * 60 * 1000)
      : null;
  const memberSince = memberSinceDate && !Number.isNaN(memberSinceDate.getTime())
    ? memberSinceDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' })
    : 'N/A';

  return (
    <div
      className="profile-page"
      style={{
        position: 'fixed',
        top: 'var(--topbar-height)',
        left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
        right: chatOpen && !chatModal ? chatWidthPx : 0,
        bottom: 0,
        overflowY: 'auto',
        padding: '2rem 2.5rem',
      }}
    >
      <div className="profile-page__container">
        {/* Header */}
        <div className="profile-page__header">
          <h1 className="profile-page__title">Profile</h1>
          <p className="profile-page__subtitle">Manage your account information</p>
        </div>

        {/* Avatar + Identity Card */}
        <div className="profile-page__card profile-page__identity-card">
          <div className="profile-page__avatar-section">
            <div className="profile-page__avatar">
              {initials}
            </div>
            <div className="profile-page__avatar-info">
              <span className="profile-page__display-name">{user?.full_name || 'Unnamed User'}</span>
              <span className="profile-page__display-email">{user?.email}</span>
              <span className="profile-page__plan-badge">
                {user?.subscription_tier?.tier_name || 'Free'} Plan
              </span>
            </div>
          </div>
        </div>

        {/* Editable Details Card */}
        <div className="profile-page__card">
          <h2 className="profile-page__card-title">Personal Information</h2>
          <p className="profile-page__card-desc">Update your personal details below.</p>

          <div className="profile-page__form">
            {/* Full Name */}
            <div className="profile-page__field">
              <label htmlFor="profile-full-name" className="profile-page__label">
                <User size={15} className="profile-page__label-icon" />
                Full Name
              </label>
              <div className="profile-page__input-row">
                <input
                  id="profile-full-name"
                  type="text"
                  className="profile-page__input"
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  placeholder="Enter your full name"
                  maxLength={120}
                  disabled={saving}
                />
              </div>
            </div>

            {/* Email (read-only) */}
            <div className="profile-page__field">
              <label className="profile-page__label">
                <Mail size={15} className="profile-page__label-icon" />
                Email Address
              </label>
              <div className="profile-page__readonly-value">{user?.email}</div>
              <p className="profile-page__field-hint">Email changes are managed through your authentication provider.</p>
            </div>

            {/* Member Since (read-only) */}
            <div className="profile-page__field">
              <label className="profile-page__label">
                <Calendar size={15} className="profile-page__label-icon" />
                Member Since
              </label>
              <div className="profile-page__readonly-value">{memberSince}</div>
            </div>
          </div>

          {/* Save / Feedback */}
          <div className="profile-page__actions">
            {feedback && (
              <div className={`profile-page__feedback profile-page__feedback--${feedback.kind}`}>
                {feedback.kind === 'success' ? <Check size={15} /> : <AlertCircle size={15} />}
                <span>{feedback.message}</span>
              </div>
            )}
            <button
              type="button"
              className="profile-page__save-btn"
              disabled={!isDirty || saving}
              onClick={handleSave}
            >
              {saving ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
              {saving ? 'Saving…' : 'Save Changes'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ProfilePage;
