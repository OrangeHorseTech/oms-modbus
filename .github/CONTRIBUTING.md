# Contributing to OMS Modbus

Thanks for your interest in contributing. OMS Modbus is developed and
maintained by [OrangeHorse Electronic Technology Co., Ltd.](https://orangehorsetech.com),
an industrial communication solutions provider.

## Before you contribute

- For **bug reports**, use the [Bug Report template](https://github.com/OrangeHorseTech/oms-modbus/issues/new?template=bug_report.md).
- For **feature requests**, use the [Feature Request template](https://github.com/OrangeHorseTech/oms-modbus/issues/new?template=feature_request.md).
- For **major changes**, please open an issue first to discuss your proposal.

## Development workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Write code and tests. All tests must pass:
   ```bash
   cargo test
   cargo clippy --all-targets -- -D warnings
   cargo fmt --check
   ```
4. Commit your changes
5. Push your branch and open a Pull Request

## Pull Request guidelines

- Keep PRs focused — one feature or fix per PR
- Add tests for new functionality
- Update documentation if public API changes
- Ensure CI checks pass (when enabled)

## Code style

- `cargo fmt` — standard Rust formatting
- `cargo clippy` — zero warnings (treated as errors)
- No `.unwrap()` in production code
- `#![forbid(unsafe_code)]` — all unsafe is banned

## License

By contributing, you agree that your contributions will be licensed
under either [MIT](../LICENSE-MIT) or [Apache 2.0](../LICENSE-APACHE),
at the user's option.
