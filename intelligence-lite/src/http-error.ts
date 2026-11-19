/** Intelligence's error envelope, the one the runtime client reads `error.code` and `error.retryable` from. */
export type ErrorBody = {
  error: {
    code: string;
    message: string;
    category: string;
    retryable: boolean;
  };
};

export function errorBody(
  code: string,
  message: string,
  category: string,
  retryable = false,
): ErrorBody {
  return { error: { code, message, category, retryable } };
}

export const NOT_ENABLED = {
  learning: errorBody(
    "ROUTE_NOT_FOUND",
    "Learning is not enabled on this intelligence-lite deployment.",
    "not_found",
  ),
  trajectories: errorBody(
    "TRAJECTORIES_NOT_ENABLED",
    "Trajectories are not enabled on this intelligence-lite deployment.",
    "permission",
  ),
  inspector: errorBody(
    "ROUTE_NOT_FOUND",
    "The inspector is not enabled on this intelligence-lite deployment.",
    "not_found",
  ),
} as const;
