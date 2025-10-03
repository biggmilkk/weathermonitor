from .nws import render as render_nws_grouped_compact
from .ec import render as render_ec_grouped_compact
from .uk import render as render_uk_grouped
from .cma import render as render_cma

RENDERERS = {
    "nws_grouped_compact": render_nws_grouped_compact,
    "ec_grouped_compact": render_ec_grouped_compact,
    "uk_grouped_compact": render_uk_grouped,
    "rss_cma": render_cma,

}
