# Support functions for bootstrapping python tools

python=${PYTHON:-python3}

# set -x would generate a lot of noise when we activate the venv, so
# we use this hand-crafted equivalent here:
run()
{
    echo "$@" 1>&2
    "$@"
}

build_dir="$thisdir/build"
venvs_dir="$build_dir/venvs"
bin_dir="$build_dir/wrappers/bin"

install_tool()
{
    tool="$1"
    shift

    pip_spec="$1"
    shift

    test -f "$bin_dir/$tool" && return

    run mkdir -p "$venvs_dir" "$bin_dir"

    venv="$venvs_dir/$tool"

    run "$python" -m venv "$venv"

    # Run in a subshell to prevent the activate/deactivate steps
    # interfering with pyenv
    (
        run source "$venv/bin/activate"
        run "$python" -m pip install -qqq --upgrade pip
        run "$python" -m pip install -qqq $pip_spec
        run ln -fs "../../venvs/$tool/bin/$tool" "$bin_dir/$tool"
        run deactivate
    )
}

run_tool()
{
    tool="$1"
    shift

    # Ensure that the tool has access to all the bootstrapped tools
    PATH="$bin_dir:$PATH"

    exec "$bin_dir/$tool" "$@"
}

install_and_run_tool()
{
    tool="$1"
    shift

    pip_spec="$1"
    shift

    install_tool "$tool" "$pip_spec"
    run_tool "$tool" "$@"
}
