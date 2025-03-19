import click


@click.command()
@click.option(
    "--trakt-client-id",
    required=True,
    envvar="TRAKT_CLIENT_ID",
)
@click.option(
    "--trakt-access-token",
    required=True,
    envvar="TRAKT_ACCESS_TOKEN",
)
def main(
    trakt_client_id: str,
    trakt_access_token: str,
) -> None:
    print("Hello, World!")


if __name__ == "__main__":
    main()
