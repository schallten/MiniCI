from main import DockerRunner

# test docker
runner = DockerRunner()
success,output = runner.run_steps(
    steps=["echo 'hello from docker'","python3 -c 'print(2+2)'"],
    run_id="test-001"
)

print(f"Success: {success}")
print(f"Output: {output}")
