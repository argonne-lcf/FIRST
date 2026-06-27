# Request Routing

This page describes the *per-request* path through the gateway: how an
inference call is validated, mapped to a model deployment, and proxied to
a backend replica. For the durable resource model behind these views, see
[Data Model](data-model.md); for how routing config is published, see
[Declarative Configuration](declarative-config.md) and the
[Controller Framework](controllers.md).

!!! note "Status"
    The admin/control routes (plan, apply, resource reads, replica
    scale) are implemented today. The user-facing inference routes
    described below — LLM dispatch through `litellm.Router`, non-LLM
    pass-through, the Redis-cached router map — are the **target
    design** that the Router Config Controller and the corresponding
    apiserver routes will materialize. See
    [Roadmap](../roadmap.md) for the current state.

## Endpoints name the task, not the model

Inference views are organized around the **task** (create responses,
segment an image), not the model or cluster. The model is a pluggable
request parameter; clients pass it in the request body (or a header) and
the gateway resolves it to a backing deployment.

A view's responsibilities are uniform:

1. Request validation.
2. Authorization.
3. Dispatch to a supporting model deployment.

Dispatch differs by family:

- **LLM routes** (Chat Completions, Responses, Anthropic Messages)
  dispatch through a `litellm.Router` instance, which handles protocol
  translation, federation, retries, fallbacks, and per-deployment
  cooldowns.
- **Other views** own their own dispatch logic — typically a thin
  pass-through proxy to whichever deployment is healthy.

By default, requests are federated and load-balanced across all live
deployments under the named model. Users do not specify which deployment
or cluster runs the request (though the response metadata does include a
hint about which deployment served it). A client that needs to pin a
specific deployment — e.g., to investigate the behavior of a particular
replica — can do so with a request header or alternative path.

## API view sequence

Every inference view, regardless of model family, performs the same
preamble:

1. Look up the `ModelDefinition` for the requested model name.
2. Authorize user access via `access_groups`.
3. Check per-user request limits.
4. Verify that this view is in the model's `supported_endpoints`.
5. Update the model's [rolling usage level](https://oneuptime.com/blog/post/2026-03-31-redis-rolling-window-rate-counter/view).

After the preamble, the view branches by family.

### LLM inference views

1. Check per-user token limits.
2. Check the model's presence in the current LiteLLM Router instance.
    - If a router entry is present, dispatch to the router and proxy the
      result back.
    - Otherwise, fall through to step 3.
3. Iterate the model's deployment specs:
    - If any deployment is active, return `503` with `Retry-After` — the
      client should auto-retry (the deployment should be in the router
      shortly).
    - If no deployment is active but one has the capacity to scale up,
      trigger it and return `503` with `Retry-After`.
    - Otherwise, return `503` *without* `Retry-After`: this model is not
      running and we have no way to make it run.

### Non-LLM inference views

1. Iterate the model's deployment specs:
    - If a healthy deployment exists, proxy the request and return.
    - If none are healthy but one is pending, return `503` with
      `Retry-After`.
    - If none are active but one can scale up, trigger it and return
      `503` with `Retry-After`.
    - Otherwise, return `503` *without* `Retry-After`.

## Router rebuild semantics

`litellm.Router` holds an **in-memory** deployment list, and under
multiple Uvicorn workers **each worker has its own Router**. High-touch
counters — both LiteLLM's own rate/cooldown state and our load-metric
in-flight set — live in **Redis**, so the per-worker routers behave
coherently. The routers themselves are **per-worker, stateless, and
rebuilt from the DB**:

- Each worker rebuilds its Router from the current configuration on every
  relevant change — not only on admin apply, but also on **replica
  add/remove**, since autoscaling churns replicas continuously.
- The rebuild trigger is the **single global config version**. Workers
  and control loops compare "the generation I built against" vs. "the
  current generation" and re-read when stale. Because the version lives
  in one place (the monotonic counter / history table) rather than
  scattered across resource rows, a single integer comparison is a
  sufficient and consistent staleness check across all the tables the
  router draws from.

The Router Config Controller (see
[Controller Framework](controllers.md#first-controllers)) is responsible
for assembling the data structure that workers read; that page describes
what gets included and how scale-down draining is coordinated.
