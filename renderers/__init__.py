from .nws import render as render_nws_grouped_compact
from .ec import render as render_ec_grouped_compact
from .uk import render as render_uk_grouped
from .cma import render as render_cma
from .meteoalarm import render as render_meteoalarm
from .bom import render as render_bom_grouped
from .jma import render as render_jma_grouped
from .pagasa import render as render_pagasa


RENDERERS = {
    "nws_grouped_compact": render_nws_grouped_compact,
    "ec_grouped_compact": render_ec_grouped_compact,
    "uk_grouped_compact": render_uk_grouped,
    "rss_cma": render_cma,
    "rss_meteoalarm": render_meteoalarm,
    "rss_bom_multi": render_bom_grouped,
    "rss_jma": render_jma_grouped,
    "rss_pagasa": render_pagasa,

}
