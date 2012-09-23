
from base import BabeBase, StreamHeader, StreamFooter

# Get the tokens from https://dev.twitter.com/docs/auth/tokens-devtwittercom

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")


def flatten_status(u):
    for p in ["author"]:
        v = getattr(u, p)
        for k in v.__dict__:
            if k == "_api":
                continue
            setattr(u, "%s_%s" % (p, k),  getattr(v, k))
    hashtags = u.entities["hashtags"]
    u.hashtags = [entity['text'] for entity in hashtags]


def build_status_names(u):
    names = u.__dict__.keys()
    names.sort()
    for bad_key in ["_api", "user", "author"]:
        try:
            names.remove(bad_key)
        except ValueError:
            pass
    return names


def pull_twitter(false_stream, consumer_key=None,
    consumer_secret=None, access_token=None, access_token_secret=None):
    import tweepy

    if consumer_key:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
    else:
        api = tweepy.API()

    # If the authentication was successful, you should
    # see the name of the account print out
    #print api.me().name

    # If the application settings are set for "Read and Write" then
    # this line should tweet out the message to your account's
    # timeline. The "Read and Write" setting is on https://dev.twitter.com/apps
    #api.update_status('Updating using OAuth authentication via Tweepy!')
    metainfo = None
    if consumer_key:
        statuses = api.user_timeline(include_entities=True)
    else:
        statuses = api.public_timeline(include_entities=True)
    for u in statuses:
        flatten_status(u)
        if not metainfo:
            names = build_status_names(u)
            metainfo = StreamHeader(typename="Status", fields=names)
            yield metainfo
        u.__class__.__iter__ = lambda s: iter([getattr(s, key) for key in names])
        yield u
    yield StreamFooter()

BabeBase.register('pull_twitter', pull_twitter)
