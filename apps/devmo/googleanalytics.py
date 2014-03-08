import logging
from datetime import timedelta
from functools import wraps

from django.conf import settings

import httplib2
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import SignedJwtAssertionCredentials


log = logging.getLogger('k.googleanalytics')


key = settings.GA_KEY
account = settings.GA_ACCOUNT
profile_id = settings.GA_PROFILE_ID


def retry_503(f):
    """Call `f`. If `f` raises an HTTP 503 exception, try again once.

    This is what Google Analytics recommends:
    https://developers.google.com/analytics/devguides/config/mgmt/v3/errors
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except HttpError as e:
            log.error('HTTP Error calling Google Analytics: %s', e)

            if e.resp.status == 503:
                return f(*args, **kwargs)

    return wrapper


def _build_request():
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    creds = SignedJwtAssertionCredentials(account, key, scope)
    request = creds.authorize(httplib2.Http())
    service = build('analytics', 'v3', request)
    return service.data().ga()


def pageviews_by_document(start_date, end_date, verbose=False):
    """Return the number of pageviews by document in a given date range.

    * Only returns en-US documents for now since that's what we did with
    webtrends.

    Returns a dict with pageviews for each document:
        {<document_id>: <pageviews>,
         1: 42,
         7: 1337,...}
    """
    from wiki.models import Document  # Circular import aversion.

    counts = {}
    request = _build_request()
    max_results = 10000

    end_date_step = end_date

    while True:  # To reduce the size of result set request 3 months at a time
        start_date_step = end_date_step - timedelta(90)

        if start_date_step < start_date:
            start_date_step = start_date

        if verbose:
            print 'Fetching data for %s to %s:' % (start_date_step,
                                                   end_date_step)

        start_index = 1

        while True:  # To deal with pagination

            @retry_503
            def _make_request():
                return request.get(
                    ids='ga:' + profile_id,
                    start_date=str(start_date_step),
                    end_date=str(end_date_step),
                    metrics='ga:pageviews',
                    dimensions='ga:pagePath',
                    filters=('ga:pagePathLevel1==/en-US/'),
                    max_results=max_results,
                    start_index=start_index).execute()

            results = _make_request()

            if verbose:
                d = (max_results - 1
                     if start_index + max_results - 1 < results['totalResults']
                     else results['totalResults'] - start_index)
                print '- Got %s of %s results.' % (start_index + d,
                                                   results['totalResults'])

            for result in results['rows']:
                path = result[0]
                pageviews = int(result[1])
                doc = Document.from_url(path, id_only=True, check_host=False)
                if not doc:
                    continue

                # The same document can appear multiple times due to url params
                counts[doc.pk] = counts.get(doc.pk, 0) + pageviews

            # Move to next page of results.
            start_index += max_results
            if start_index > results['totalResults']:
                break

        end_date_step = start_date_step - timedelta(1)

        if start_date_step == start_date or end_date_step < start_date:
            break

    return counts
