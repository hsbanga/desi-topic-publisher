"""Runs on GitHub Actions every 15 minutes. Publishes queued Instagram posts
that are due. The Instagram token is stored encrypted in state/token.enc; the
key is the TOKEN_KEY repository secret. The token is never printed."""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from cryptography.fernet import Fernet

GRAPH = 'https://graph.instagram.com/v24.0'
GRACE_HOURS = 12
REFRESH_AFTER_DAYS = 7
TOKEN_FILE = Path('state/token.enc')
META_FILE = Path('state/meta.json')
QUEUE_DIR = Path('queue')
RESULTS_DIR = Path('results')


def utcnow():
    return datetime.now(timezone.utc)


def parse_utc(value):
    d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def read_json(path, default):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return default


def write_json(path, obj):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True), encoding='utf-8')


class Instagram:
    def __init__(self, token, user_id):
        self.token = token
        self.user_id = user_id

    def scrub(self, text):
        return str(text).replace(self.token, '***')

    def request(self, method, path, **kw):
        url = path if path.startswith('http') else f'{GRAPH}/{path}'
        try:
            r = requests.request(method, url, timeout=60, **kw)
        except requests.RequestException as e:
            raise RuntimeError(self.scrub(f'network error: {e}'))
        try:
            body = r.json()
        except Exception:
            body = {}
        if not r.ok:
            msg = (body.get('error') or {}).get('message') or r.text[:200]
            raise RuntimeError(self.scrub(f'HTTP {r.status_code}: {msg}'))
        return body

    def create(self, params):
        data = dict(params, access_token=self.token)
        return self.request('POST', f'{self.user_id}/media', data=data)['id']

    def wait_ready(self, container_id, tries=36, delay=5):
        for _ in range(tries):
            st = self.request('GET', container_id, params={'fields': 'status_code', 'access_token': self.token})
            code = st.get('status_code')
            if code == 'FINISHED':
                return
            if code in ('ERROR', 'EXPIRED'):
                raise RuntimeError(f'Instagram could not process the media ({code})')
            time.sleep(delay)
        raise RuntimeError('Timed out waiting for Instagram to process the media')

    def publish(self, creation_id):
        return self.request('POST', f'{self.user_id}/media_publish',
                            data={'creation_id': creation_id, 'access_token': self.token})['id']

    def permalink(self, media_id):
        try:
            return self.request('GET', media_id, params={'fields': 'permalink', 'access_token': self.token}).get('permalink', '')
        except RuntimeError:
            return ''


def publish_item(ig, item):
    images = item.get('images') or []
    if item.get('media_type', 'carousel') != 'carousel' or not (2 <= len(images) <= 10):
        raise RuntimeError('Cloud publishing supports carousels of 2-10 images only')
    children = []
    for url in images:
        cid = ig.create({'image_url': url, 'is_carousel_item': 'true'})
        ig.wait_ready(cid)
        children.append(cid)
    parent = ig.create({'media_type': 'CAROUSEL', 'children': ','.join(children), 'caption': item.get('caption', '')})
    ig.wait_ready(parent)
    started = utcnow()
    try:
        media_id = ig.publish(parent)
    except Exception:
        # an error here does not prove it failed (e.g. a timeout after Instagram
        # accepted it) -- look before reporting a failure, or a retry posts twice
        live = find_live(ig, item, started - timedelta(minutes=5))
        if not live:
            raise
        media_id = live['id']
    return media_id, ig.permalink(media_id)


def find_live(ig, item, since):
    """The recent post with this item's caption, or None."""
    key = ' '.join(str(item.get('caption') or '').split())[:80]
    if not key:
        return None
    for wait in (0, 8, 20):
        time.sleep(wait)
        try:
            media = ig.request('GET', f'{ig.user_id}/media', params={
                'fields': 'id,caption,timestamp,permalink', 'limit': 25, 'access_token': ig.token}).get('data', [])
        except RuntimeError:
            continue
        for m in media:
            if ' '.join(str(m.get('caption') or '').split())[:80] != key:
                continue
            try:
                if parse_utc(m.get('timestamp', '').replace('+0000', '+00:00')) < since:
                    continue
            except ValueError:
                pass
            return m
    return None


def _git(*args):
    return subprocess.run(args, capture_output=True, text=True)


def git_pull():
    """Latest queue from the app (a cancel = the queue file was deleted)."""
    if os.environ.get('NO_GIT'):
        return True
    return _git('git', 'pull', '--rebase', '-X', 'theirs').returncode == 0


def git_commit(message):
    """Commit + push immediately (rebasing on conflict) so a crash later in
    the run can never cause an already-published post to be published again.
    True when the change is on GitHub."""
    if os.environ.get('NO_GIT'):
        return True
    _git('git', 'config', 'user.name', 'publisher-bot')
    _git('git', 'config', 'user.email', 'publisher-bot@users.noreply.github.com')
    _git('git', 'add', 'results', 'state')
    if _git('git', 'diff', '--cached', '--quiet').returncode == 0:
        return True
    _git('git', 'commit', '-m', message)
    for _ in range(4):
        if _git('git', 'push').returncode == 0:
            return True
        if _git('git', 'pull', '--rebase', '-X', 'theirs').returncode != 0:
            _git('git', 'rebase', '--abort')
    print('WARNING: could not push state', file=sys.stderr)
    return False


def main(now=None, publisher=publish_item, finder=find_live):
    now = now or utcnow()
    key = os.environ.get('TOKEN_KEY', '').encode()
    if not key or not TOKEN_FILE.exists():
        print('Not configured: missing TOKEN_KEY or state/token.enc')
        return 1
    meta = read_json(META_FILE, {})
    due, missed, interrupted = [], [], []
    for f in sorted(QUEUE_DIR.glob('*.json')):
        item = read_json(f, None)
        if not item:
            continue
        done = read_json(RESULTS_DIR / f.name, None)
        if done is not None:
            # runs never overlap (workflow concurrency), so a leftover 'publishing'
            # claim means an earlier run died mid-publish: find out, never re-post blindly
            if done.get('status') == 'publishing':
                interrupted.append((f, item, done))
            continue
        try:
            when = parse_utc(item['when_utc'])
        except Exception:
            write_json(RESULTS_DIR / f.name, {'id': item.get('id'), 'status': 'failed', 'error': 'Unreadable time', 'at': now.isoformat()})
            continue
        if when > now:
            continue
        (missed if now - when > timedelta(hours=GRACE_HOURS) else due).append((f, item))

    try:
        issued = parse_utc(meta['issued_at'])
    except Exception:
        issued = None
    need_refresh = issued is None or now - issued >= timedelta(days=REFRESH_AFTER_DAYS)
    if not due and not missed and not interrupted and not need_refresh:
        print('Nothing to do.')
        return 0

    fernet = Fernet(key)
    token = fernet.decrypt(TOKEN_FILE.read_bytes()).decode()
    print('::add-mask::' + token)
    ig = Instagram(token, meta.get('user_id', ''))

    for f, item in missed:
        write_json(RESULTS_DIR / f.name, {'id': item['id'], 'status': 'missed', 'at': now.isoformat(),
                                          'error': f'Overdue by more than {GRACE_HOURS}h; not posted'})
    if missed:
        git_commit('missed posts')

    for f, item, claim in interrupted:
        try:
            since = parse_utc(claim.get('at')) - timedelta(minutes=10)
        except Exception:
            since = now - timedelta(days=2)
        live = finder(ig, item, since)
        result = {'id': item['id'], 'at': now.isoformat()}
        if live:
            result.update(status='published', media_id=live.get('id', ''), permalink=live.get('permalink', ''))
        else:
            result.update(status='failed', error='An earlier run stopped while publishing and the post is not on Instagram.')
        write_json(RESULTS_DIR / f.name, result)
        git_commit(f"recovered {item['id']}")

    for f, item in due:
        # re-read the queue right before posting: the user may have cancelled it
        git_pull()
        if not f.exists():
            print(f"Cancelled {item['id']}")
            continue
        # claim it on GitHub BEFORE posting, so a crash or a failed push later can
        # never lead to a second post of the same item
        write_json(RESULTS_DIR / f.name, {'id': item['id'], 'status': 'publishing', 'at': utcnow().isoformat()})
        if not git_commit(f"publishing {item['id']}"):
            (RESULTS_DIR / f.name).unlink()
            print(f"Skipped {item['id']}: could not record the claim; will retry next run")
            continue
        result = {'id': item['id'], 'at': now.isoformat()}
        try:
            media_id, permalink = publisher(ig, item)
            result.update(status='published', media_id=media_id, permalink=permalink)
            print(f"Published {item['id']}")
        except Exception as e:
            result.update(status='failed', error=ig.scrub(e)[:300])
            print(f"Failed {item['id']}: {result['error']}")
        write_json(RESULTS_DIR / f.name, result)
        git_commit(f"result {item['id']}")

    if need_refresh:
        try:
            r = ig.request('GET', 'https://graph.instagram.com/refresh_access_token',
                           params={'grant_type': 'ig_refresh_token', 'access_token': token})
            new = r.get('access_token')
            if new:
                TOKEN_FILE.write_bytes(fernet.encrypt(new.encode()))
                meta['issued_at'] = now.isoformat()
                write_json(META_FILE, meta)
                print('Token refreshed')
        except RuntimeError as e:
            print('Token refresh skipped:', ig.scrub(e))
        git_commit('token refresh')
    return 0


if __name__ == '__main__':
    sys.exit(main())
