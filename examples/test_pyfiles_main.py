"""
Main script for testing raydp-submit --py-files functionality.
This script imports and uses functions from test_pyfile.py.
"""
import sys
import raydp


def main():
    """Test that py-files are properly distributed and accessible."""
    print("=" * 60)
    print("Testing raydp-submit --py-files functionality")
    print("=" * 60)

    # JDK 17+ requires --add-opens for reflective access and --add-exports for direct access
    # to internal JDK modules. These are needed for Spark, Ray serialization, and RayDP.
    java_opts = " ".join([
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.math=ALL-UNNAMED",
        "--add-opens=java.base/java.text=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ])
    extra_configs = {
        "spark.executor.extraJavaOptions": java_opts,
        "spark.driver.extraJavaOptions": java_opts,
        "spark.ray.raydp_app_master.extraJavaOptions": java_opts,
    }
    spark = raydp.init_spark("Test PyFiles", 1, 1, "500M",
                             configs=extra_configs)

    # Test: Use compute_sum in Spark executor context
    print("\nTesting py-files in Spark executor context...")
    # Create RDD and use function from test_pyfile in executors
    numbers_rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)

    # Map function that uses imported function
    def process_partition(partition):
        from test_pyfile import compute_sum  # pylint: disable=import-outside-toplevel
        nums = list(partition)
        return [compute_sum(nums)]

    results = numbers_rdd.mapPartitions(process_partition).collect()
    total = sum(results)
    print(f"Sum from executors: {total}")
    assert total == 15, f"Expected 15, got {total}"
    print("✓ Functions from py-files work in Spark executors!")

    raydp.stop_spark()

    print("\n" + "=" * 60)
    print("Test passed! --py-files is working correctly!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
