"""Allows for interaction with a BitBucket repository.

The `BitBucket` class in this collection is a storage block that lets Prefect agents
pull Prefect flow code from BitBucket repositories.

The `BitBucket` block is ideally configured via the Prefect UI, but can also be used
in Python as the following examples demonstrate.

Examples
```python
from prefect_bitbucket.repository import BitBucketRepository

# public BitBucket repository
public_bitbucket_block = BitBucketRepository(
    repository="https://bitbucket.com/my-project/my-repository.git"
)

public_bitbucket_block.save(name="my-bitbucket-block")

# specific branch or tag
branch_bitbucket_block = BitBucketRepository(
    reference="branch-or-tag-name",
    repository="https://bitbucket.com/my-project/my-repository.git"
)

branch_bitbucket_block.save(name="my-bitbucket-block")

# private BitBucket repository
private_bitbucket_block = BitBucketRepository(
    repository="https://bitbucket.com/my-project/my-repository.git",
    bitbucket_credentials=BitBucketCredentials.load("my-bitbucket-credentials-block")
)

private_bitbucket_block.save(name="my-private-bitbucket-block")

"""

import io
import os
import stat
import tempfile
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Union
from urllib.parse import quote, urlparse

from pydantic import Field, model_validator
from typing_extensions import Self

from prefect.exceptions import InvalidRepositoryURLError
from prefect.filesystems import ReadableDeploymentStorage
from prefect.utilities.asyncutils import sync_compatible
from prefect.utilities.processutils import run_process
from prefect_bitbucket.credentials import BitBucketCredentials


class BitBucketRepository(ReadableDeploymentStorage):
    """Interact with files stored in BitBucket repositories.

    An accessible installation of git is required for this block to function
    properly.
    """

    _block_type_name = "BitBucket Repository"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/5d729f7355fb6828c4b605268ded9cfafab3ae4f-250x250.png"  # noqa
    _description = "Interact with files stored in BitBucket repositories."

    repository: str = Field(
        default=...,
        description="The URL of a BitBucket repository to read from in HTTPS format",
    )
    reference: Optional[str] = Field(
        default=None,
        description="An optional reference to pin to; can be a branch or tag.",
    )
    bitbucket_credentials: Optional[BitBucketCredentials] = Field(
        default=None,
        description=(
            "An optional BitBucketCredentials block for authenticating with "
            "private BitBucket repos."
        ),
    )

    @model_validator(mode="after")
    def _ensure_credentials_go_with_https(self) -> Self:
        """Ensure that credentials are not provided with 'SSH' formatted BitBucket URLs.

        Validators are by default only called on provided arguments.

        Note: validates `credentials` specifically so that it only fires when private
        repositories are used.
        """
        if self.bitbucket_credentials is not None:
            if urlparse(self.repository).scheme != "https":
                raise InvalidRepositoryURLError(
                    (
                        "Credentials can only be used with BitBucket repositories "
                        "using the 'HTTPS' format. You must either remove the "
                        "credential if you wish to use the 'SSH' format and are not "
                        "using a private repository, or you must change the repository "
                        "URL to the 'HTTPS' format."
                    )
                )

        return self

    def _create_credential_helper_script(self) -> Optional[str]:
        """Create a temporary script for GIT_ASKPASS that provides credentials securely."""
        if self.bitbucket_credentials is None:
            return None

        token = self.bitbucket_credentials.token.get_secret_value()
        username = self.bitbucket_credentials.username

        if username is None:
            username = "x-token-auth"

        # Encode special characters in username and token
        safe_username = quote(username or "")
        safe_token = quote(token or "")
        credential_string = f"{safe_username}:{safe_token}"

        script_content = f'''#!/bin/bash
echo "{credential_string}"
'''

        fd, script_path = tempfile.mkstemp(suffix=".sh", prefix="git_askpass_")
        with os.fdopen(fd, "w") as f:
            f.write(script_content)

        os.chmod(script_path, stat.S_IRWXU)
        return script_path

    @staticmethod
    def _get_paths(
        dst_dir: Union[str, None], src_dir: str, sub_directory: Optional[str]
    ) -> Tuple[str, str]:
        """Return the fully formed paths for BitBucketRepository contents.

        Return will take the form of (content_source, content_destination).

        """
        if dst_dir is None:
            content_destination = Path(".").absolute()
        else:
            content_destination = Path(dst_dir)

        content_source = Path(src_dir)

        if sub_directory:
            content_destination = content_destination.joinpath(sub_directory)
            content_source = content_source.joinpath(sub_directory)

        return str(content_source), str(content_destination)

    @sync_compatible
    async def get_directory(
        self, from_path: Optional[str] = None, local_path: Optional[str] = None
    ) -> None:
        """Clones a BitBucket project within `from_path` to the provided `local_path`.

        This defaults to cloning the repository reference configured on the
        Block to the present working directory.

        Args:
            from_path: If provided, interpreted as a subdirectory of the underlying
                repository that will be copied to the provided local path.
            local_path: A local path to clone to; defaults to present working directory.

        """
        # Construct command
        cmd = ["git", "clone", self.repository]
        if self.reference:
            cmd += ["-b", self.reference]

        # Limit git history
        cmd += ["--depth", "1"]

        # Clone to a temporary directory and move the subdirectory over
        with TemporaryDirectory(suffix="prefect") as tmp_dir:
            cmd.append(tmp_dir)

            env = {}
            credential_script = self._create_credential_helper_script()
            if credential_script:
                env["GIT_ASKPASS"] = credential_script
                env["GIT_TERMINAL_PROMPT"] = "0"

            try:
                err_stream = io.StringIO()
                out_stream = io.StringIO()
                process = await run_process(
                    cmd, stream_output=(out_stream, err_stream), env=env
                )
                if process.returncode != 0:
                    err_stream.seek(0)
                    raise OSError(f"Failed to pull from remote:\n {err_stream.read()}")
            finally:
                if credential_script and os.path.exists(credential_script):
                    os.unlink(credential_script)

            content_source, content_destination = self._get_paths(
                dst_dir=local_path, src_dir=tmp_dir, sub_directory=from_path
            )

            copytree(src=content_source, dst=content_destination, dirs_exist_ok=True)
