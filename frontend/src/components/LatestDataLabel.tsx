import { useEffect, useState } from 'react';
import { getMaxDataDate } from '../services/backendClient';

function formatMaxDataDateLabel(iso: string): string {
  const parts = iso.split('-').map(Number);
  if (parts.length !== 3 || parts.some((n) => Number.isNaN(n))) return iso;
  const [y, m, d] = parts;
  const dt = new Date(y, m - 1, d);
  return dt.toLocaleDateString(undefined, { dateStyle: 'medium' });
}

/** Shows the latest calendar date available in analytics (left-aligned in the dashboard toolbar). */
const LatestDataLabel = () => {
  const [label, setLabel] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMaxDataDate()
      .then(({ max_date }) => {
        if (!cancelled) setLabel(formatMaxDataDateLabel(max_date));
      })
      .catch(() => {
        if (!cancelled) setLabel(null);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!label) return null;

  return (
    <span
      className="latest-data-label shrink-0"
      title="Latest date present in the connected analytics dataset. Preset ranges are anchored to this boundary."
      aria-live="polite"
    >
      Latest data: {label}
    </span>
  );
};

export default LatestDataLabel;
