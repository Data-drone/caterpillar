# Copyright (C) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import os
import shutil
import tempfile

import pytest


@pytest.fixture
def index_dir(request):
    path = tempfile.mkdtemp()

    def clean():
        shutil.rmtree(path)

    request.addfinalizer(clean)
    return os.path.join(path, "test_index")
