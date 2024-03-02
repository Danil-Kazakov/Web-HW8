from typing import List, Any

import redis
from redis_lru import RedisLRU

from models import Author, Quote

client = redis.StrictRedis(host="localhost", port=6379, password=None)
cache = RedisLRU(client)


# @cache
def find_by_tag(tag: str) -> list[str | None]:
    quotes = Quote.objects(tags__iregex=tag)
    result = [q.quote for q in quotes]
    return result


# @cache
def find_by_author(author: str) -> list[list[Any]]:
    authors = Author.objects(fullname__iregex=author)
    result = {}
    for a in authors:
        quotes = Quote.objects(author=a)
        result[a.fullname] = [q.quote for q in quotes]
    return result


if __name__ == '__main__':
    print("Hello, print 'help' to see the commands: ")
    while True:
        command = input().strip()
        if command == "help":
            print("Commands:")
            print("  name: <author> - Find quotes by author")
            print("  tag: <tag> - Find quotes by tag")
            print("  tags: <tag1>,<tag2>,... - Find quotes by multiple tags")
            print("  exit - Exit the program")
        elif command.startswith("name"):
            author = command[len("name:"):].strip()
            print(find_by_author(author))
        elif command.startswith("tag"):
            tag = command[len("tag:"):].strip()
            print(find_by_tag(tag))
        elif command.startswith(("tags")):
            tags = command[len("tags:"):].strip().split(",")
            all_quotes = []
            for tag in tags:
                all_quotes.extend(find_by_tag(tag))
            print(all_quotes)
        elif command.startswith("exit"):
            break
        else:
            print("Unknown command")