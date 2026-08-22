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
| 429 | `exceeded.html` | proxy, and standalone bench | The site is over its rate limit. |
| 500 | `internal_server_error.html` | bench | The site raised an exception. |
| 502 | `bad_gateway.html` | bench | Gunicorn is down. |
| 503 | `deactivated.html` | proxy, port 10091 | The site is deactivated. |
| 504 | `gateway_timeout.html` | bench | Gunicorn passed `http_timeout`. |

The proxy sends a suspended, deactivated or unknown site to a local upstream on
127.0.0.1. That upstream returns the status, and the proxy serves the page for it.

The proxy does not intercept 502 and 504. A 502 or 504 from the bench passes through
it and reaches the visitor. A 502 or 504 that the proxy makes itself means that the
application server is down or slow, and NGINX serves its own default page. This split
keeps a bench failure and a server failure apart. If the proxy itself is down, the
visitor gets no page at all, because there is no HTTP response.

Two rules control how these blocks work:

- `error_page 502 /bad_gateway.html` keeps the 502 status. A page location is
  `internal`, so a visitor cannot request it directly.
- `proxy_intercept_errors on` only affects a status that has an `error_page` line.
  Every other status passes through.

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
