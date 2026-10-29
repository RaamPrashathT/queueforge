// Explicit job lifecycle states and valid transitions.
//
//   delayed -> queued -> leased -> succeeded
//                       |       -> failed --(retry)--> delayed
//                       |       \-> failed --(attempts exhausted)--> dead
//                       \-> queued   (lease expired, reaped)
//
// All transitions happen inside guarded UPDATEs (WHERE state = <expected>)
// so the transition itself is atomic and two workers can never both move
// the same row.

export const JobState = {
  Delayed: 'delayed',
  Queued: 'queued',
  Leased: 'leased',
  Succeeded: 'succeeded',
  Failed: 'failed',
  Dead: 'dead',
} as const;

export type JobState = (typeof JobState)[keyof typeof JobState];

export const TERMINAL_STATES: ReadonlySet<JobState> = new Set([
  JobState.Succeeded,
  JobState.Dead,
]);

export type AttemptOutcome = 'completed' | 'failed' | 'crashed' | 'lease_expired';
