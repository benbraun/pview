# Smoother live position updates during shade motion

2026-07-19

## Problem

While a shade is moving, pview publishes interpolated positions every
250ms (4 updates/sec). Cards that render position directly from HA state
(e.g. enhanced-shutter-card, which does no client-side tweening) animate
in visible steps.

## Design

All changes are in `spawn_position_interpolation` in
`src/commands/serve_mqtt.rs`:

- Tick every 100ms (named constant `INTERPOLATION_TICK`) instead of a
  hardcoded 250ms. A typical shade crosses ~1% of travel every
  100-300ms, so 100ms captures essentially every integer percent step —
  the practical ceiling given HA's integer position contract.
- Extract the interpolation math into a pure, unit-tested function
  `interpolate_pct(start, target, t) -> u8` with `t` clamped to 0..=1
  (today the formula is duplicated inline for the primary and secondary
  rails).
- Per rail, remember the last published percent and only publish when
  the integer value changes, so MQTT/HA traffic stays roughly flat
  despite the 2.5x tick rate.

Unchanged: behavior at ETA expiry, the `MotionStopped` snap to actual
position, best-effort (`let _ =`) publishes, and cancellation via
`AbortHandle`.

## Out of scope

Card-side CSS tweening in enhanced-shutter-card (deferred: that repo is
being actively worked on). Sub-integer positions (HA discards them).

## Testing

TDD unit tests for `interpolate_pct`: endpoints, midpoint both
directions, clamping beyond t=1. Tick/dedup wiring verified by review;
no live-broker harness exists.
