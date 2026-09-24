# NGINX configuration

The agent writes every NGINX file on a server from the templates in this directory.
Do not edit the rendered files on a server. The next render overwrites them.

## Templates and their targets

| Template | Rendered to | Linked from | Written by |
| --- | --- | --- | --- |
| `nginx/nginx.conf.jinja2` | `<agent>/nginx/nginx.conf` | `/etc/nginx/nginx.conf` | `Server._generate_nginx_config` |
| `agent/nginx.conf.jinja2` | `<agent>/nginx.conf` | `/etc/nginx/conf.d/agent.conf` | `Server._generate_agent_nginx_config` |
| `proxy/nginx.conf.jinja2` | `<agent>/nginx/proxy.conf` | `/etc/nginx/conf.d/proxy.conf` | `Proxy._generate_proxy_config` |
| `bench/nginx.conf.jinja2` | `<bench>/nginx.conf` | `include <benches>/*/nginx.conf` | `Bench.generate_nginx_config` |

The press playbooks make the symbolic links when they set up the server.
`nginx/nginx.conf.jinja2` holds the `http` block. It includes the other three files.
A proxy server has `proxy.conf` and no bench files. An application server has one
bench file for each bench and no `proxy.conf`.

The agent does not call `nginx -s reload` directly. It sends a request to
`NginxReloadManager` (`agent/nginx_reload_manager.py`), which collects the requests
from a Redis queue and reloads once for the batch.

## Request path

A normal site request passes through three programs:

1. The proxy server NGINX (`proxy.conf`) selects the upstream from the `$host` maps.
2. The application server NGINX (`<bench>/nginx.conf`) serves static files and public
   files. It sends everything else to the bench.
3. Gunicorn runs the site.

A standalone bench has no proxy in front of it. Its DNS points at the application
server, so step 1 does not happen. The template renders a different server block for
this case, behind `{% if standalone %}`.

## Error pages

The HTML files live in `agent/pages`. One copy on the server serves every bench, so a
page cannot contain a site name or a bench name. The `internal_server_error.html` page
needs the bench name for a log link, so the bench template substitutes the
`__BENCH_NAME__` placeholder with `sub_filter`.

The layer that owns a failure serves the page for it:

| Status | Page | Served by | Meaning |
| --- | --- | --- | --- |
| 402 | `suspended.html`, `suspended_saas.html` | proxy, ports 10092 and 10093 | The site is suspended. |
| 429 | `exceeded.html` | bench | The site reached its daily usage limit. |
| 500 | `internal_server_error.html` | bench | The site raised an exception. |
| 502 | `bad_gateway.html` | bench | Gunicorn is down. |
| 502 | `server_unreachable.html` | proxy | The application server is down. |
| 503 | `deactivated.html` | proxy, port 10091 | The site is deactivated. |
| 504 | `gateway_timeout.html` | bench | Gunicorn passed `http_timeout`. |
| 504 | `server_timeout.html` | proxy | The application server did not answer in time. |

The proxy sends a suspended, deactivated or unknown site to a local upstream on
127.0.0.1. That upstream returns the status, and the proxy serves the page for it.

Each layer serves a page for its own failures only. The bench owns 429, 500, 502 and
504, because all four come from the site or from gunicorn. The proxy owns 402, 503 and
the 502 and 504 that it makes itself. A 502 or 504 from the proxy means that it never
reached the application server, so the site is not the cause. The bench pages name the
site, and the proxy pages name the server. If the proxy itself is down, the visitor
gets no page at all, because there is no HTTP response.

Three rules of NGINX control how these blocks work:

- `error_page` always catches a status that NGINX makes itself. A refused upstream
  makes 502. A slow upstream makes 504. `limit_req` makes 429.
- `proxy_intercept_errors on` adds the responses of the upstream to that. The bench
  needs it, because gunicorn sends the 429 of the rate limiter of Frappe. The proxy
  keeps it off, so that the pages of the bench pass through it.
- `error_page 502 /bad_gateway.html` keeps the 502 status. A page location is
  `internal`, so a visitor cannot request it directly.

Interception throws the response body away. Do not turn it on at the proxy for a
status that the bench answers itself. The visitor then gets the page of the proxy for
a failure of the bench, and the two layers become impossible to tell apart.
`$upstream_status` cannot separate them. NGINX sets it to 502 both when the bench
answers 502 and when the connection to the bench is refused.

Status 500 uses `sub_filter` and not `error_page`. Interception discards the response
body, which would also remove the error pages of Frappe and the tracebacks of the API.

## Test a template change

The templates are Jinja2, so a syntax error only appears after a render. Check a change
before you deploy it:

1. Render the template with Jinja2 and a sample context. Render `bench/nginx.conf.jinja2`
   two times, once with `standalone=True` and once with `standalone=False`.
2. Put the output in an `http` block, and run `nginx -t` on it.
3. Ignore the errors about the certificates, the ports and the log paths. Read the line
   that reports the syntax.

A local NGINX has no `headers-more` module. Remove the `more_set_headers` lines before
the test, or build the module first.

A change to an error page needs more than `nginx -t`. Start a local NGINX with the
rendered config, and make each status happen:

1. To get a 502, point the upstream at a port that nothing listens on.
2. To get a 504, point the upstream at a server that sleeps, and lower
   `proxy_read_timeout`.
3. To get a status from the upstream itself, point the upstream at a small HTTP server
   that returns that status with a body of its own. The body tells you if the layer
   passed the response through, or replaced it with a page.

Run the config of the bench and the config of the proxy separately. A page can only
reach a visitor if every layer above it passes the response through.
