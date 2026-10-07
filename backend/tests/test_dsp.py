import math

from app.dsp import analyze_all_segments, analyze_segment, default_params, group_chunks_by_rate
from tests.synthetic import CHANNELS, coefficients, make_chunks

# Raw blocks are stored as float32 (docs/analysis-conventions.md §1), so the
# quantization noise floor at magnitude ~300 is ~2e-7. Tolerances use 1e-5:
# tight enough to prove the numerical conventions, loose enough for float32.
TOL = 1e-5


def test_known_harmonics_thd_and_true_rms():
    chunks = make_chunks(sample_chunks=(720,))
    segment = group_chunks_by_rate(
        [{k: v for k, v in chunks[0].items() if k != "raw"}], {0: chunks[0]["raw"]}
    )
    result = analyze_segment(segment[0]["data"], CHANNELS, 6000.0, coefficients(), default_params())
    va = result["channels"]["Va"]
    assert abs(va["rms"] - math.sqrt(300**2 + 10**2 + 20**2)) < TOL
    assert abs(va["thd_percent"] - math.sqrt(500) / 3) < TOL
    assert result["quality_status"] == "ok"
    harmonic_rms = {item["order"]: item["rms"] for item in va["windows"][0]["harmonics"]}
    assert abs(harmonic_rms[1] - 300.0) < TOL
    assert abs(harmonic_rms[3] - 10.0) < TOL
    assert abs(harmonic_rms[5] - 20.0) < TOL


def test_balanced_three_phase_sequence_components():
    chunks = make_chunks(sample_chunks=(720,))
    segment = group_chunks_by_rate(
        [{k: v for k, v in chunks[0].items() if k != "raw"}], {0: chunks[0]["raw"]}
    )
    result = analyze_segment(segment[0]["data"], CHANNELS, 6000.0, coefficients(), default_params())
    seq = result["symmetrical_components"]["voltage"][0]
    assert seq["status"] == "ok"
    assert abs(seq["positive_rms"] - 300.0) < TOL
    assert seq["negative_rms"] < TOL
    assert abs(seq["zero_rms"]) < TOL
    assert seq["unbalance_percent"] < TOL


def test_reversed_polarity_is_negative_sequence():
    chunks = make_chunks(sample_chunks=(720,), reversed_a=True)
    segment = group_chunks_by_rate(
        [{k: v for k, v in chunks[0].items() if k != "raw"}], {0: chunks[0]["raw"]}
    )
    result = analyze_segment(segment[0]["data"], CHANNELS, 6000.0, coefficients(), default_params())
    seq = result["symmetrical_components"]["voltage"][0]
    # Fortescue (docs/analysis-conventions.md §6) on Va=-300∠0°, Vb=300∠-120°,
    # Vc=300∠+120°: V1=100, V2=200, V0=200 (V0=(Va+Vb+Vc)/3=-200).
    assert abs(seq["negative_rms"] - 200.0) < TOL
    assert abs(seq["zero_rms"] - 200.0) < TOL
    assert abs(seq["positive_rms"] - 100.0) < TOL


def test_sample_rate_change_remains_segmented_warning():
    from tests.synthetic import make_rate_change_chunks

    raw_chunks = make_rate_change_chunks()
    segments = group_chunks_by_rate(
        [{k: v for k, v in item.items() if k != "raw"} for item in raw_chunks],
        {0: raw_chunks[0]["raw"], 1: raw_chunks[1]["raw"]},
    )
    assert len(segments) == 2
    result = analyze_all_segments(segments, coefficients(), default_params(), sample_rate_changed=True)
    assert result["quality_status"] == "warning"
    assert any(item["code"] == "sample_rate_changed" for item in result["quality"])
