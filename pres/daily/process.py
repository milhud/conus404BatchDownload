import xarray as xr
from pathlib import Path

INPUT_DIR = Path(".")          # directory containing .nc files
OUTPUT_FILE = Path("combined.nc") # output filename

def combine_nc_files(input_dir, output_file):
    files = sorted(input_dir.glob("*.nc"))
    if not files:
        print("No .nc files found.")
        return

    print(f"Found {len(files)} .nc files:")
    for f in files:
        print(f"  {f.name}")

    try:
        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            parallel=True,
            engine="h5netcdf"
        )
        ds.to_netcdf(output_file)
        ds.close()
        print(f"Combined file saved as: {output_file}")
    except Exception as e:
        print(f"Error combining files: {e}")

if __name__ == "__main__":
    combine_nc_files(INPUT_DIR, OUTPUT_FILE)

