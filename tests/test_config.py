import typing

import pytest
import saru


TEST_VALUE = "test_value"


class ConfigFactory(typing.Protocol):
    @staticmethod
    def new_config() -> saru.Config: ...


class BaseConfigFactory(ConfigFactory):
    @staticmethod
    def new_config() -> saru.Config:
        return saru.BaseConfig(saru.NullConfigBackend())


class SubConfigFactory(ConfigFactory):
    @staticmethod
    def new_config() -> saru.Config:
        c = saru.BaseConfig(saru.NullConfigBackend())
        return c.sub("parent", ensure_exists=True)


NULL_BACKEND_FACTORIES = [
    BaseConfigFactory(),
    SubConfigFactory()
]


@pytest.mark.parametrize("cf", NULL_BACKEND_FACTORIES)
class TestConfigInterface:
    """
    Generic tests for the configuration interface. Add config factories
    to ensure that all Config implementations follow expected behavior.
    """
    def test_pathget(self, cf: ConfigFactory) -> None:
        c = cf.new_config()

        c.root["a"] = {
            "b": {
                "c": TEST_VALUE
            }
        }

        assert c["a/b/c"] == TEST_VALUE

    def test_pathset(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c["a/b/c"] = TEST_VALUE
        assert c.root["a"]["b"]["c"] == TEST_VALUE

    def test_pathdel(self, cf: ConfigFactory) -> None:
        c = cf.new_config()

        c.root["a"] = {
            "b": {
                "c": TEST_VALUE
            }
        }

        del c["a/b/c"]

        assert "a" in c.root
        assert "b" in c.root["a"]
        assert c.root["a"]["b"] == {}

    def test_len(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c.root["a"] = TEST_VALUE
        c.root["b"] = TEST_VALUE
        c.root["c"] = TEST_VALUE

        assert len(c) == len(c.root)

    def test_iter(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c.root["a"] = TEST_VALUE
        c.root["b"] = TEST_VALUE
        c.root["c"] = TEST_VALUE

        assert [x for x in c] == ["a", "b", "c"]

    def test_contains(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c.root["a"] = {
            "b": {
                "c": TEST_VALUE
            }
        }

        assert "a" in c
        assert "a/b" in c
        assert "a/b/c" in c
        assert "b" not in c
        assert "a/b/d" not in c
        assert "a/c" not in c
        assert "a/b/c/d" not in c

    def test_normalized_paths(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c["a/b/c"] = TEST_VALUE

        # Config objects should respect path normalization.
        assert c["a/b/c"] == c["/a/b/c"]
        assert c["a/b/c"] == c["a/b/c/"]
        assert c["a/b/c"] == c["/a/b/c/"]
        assert c["a/b/c"] == c["///a//b/////c//"]

    def test_empty_path_error(self, cf: ConfigFactory) -> None:
        c = cf.new_config()

        # Use of blank strings is not allowed.
        with pytest.raises(saru.ConfigPathException):
            c[""] = TEST_VALUE

    def test_pathset_collision(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c["a/b/c"] = TEST_VALUE

        # a/b/c is a str, so we shouldn't be able to set a/b/c/d.
        with pytest.raises(saru.ConfigPathException):
            c["a/b/c/d"] = TEST_VALUE

    def test_pathget_missing(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c["a/b"] = TEST_VALUE
        assert "a" in c.root
        assert "b" in c.root["a"]

        # If everything but suffix exists, throw KeyError
        with pytest.raises(KeyError):
            _ = c["a/suffix"]

        # If path runs dry before reaching suffix, throw ConfigPathError
        with pytest.raises(saru.ConfigPathException):
            _ = c["a/nonexist/suffix"]

    def test_subconfig_pathget(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        sub = c.sub("subconf")

        c.root["subconf"]["value"] = TEST_VALUE
        assert sub["value"] == c["subconf/value"]

    def test_subconfig_pathset(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        sub = c.sub("subconf")
        sub["a/b/c"] = TEST_VALUE

        assert c.root["subconf"]["a"]["b"]["c"] == sub.root["a"]["b"]["c"]

    def test_subconfig_pathdel(self, cf: ConfigFactory) -> None:
        c = cf.new_config()

        c.root["subconf"] = {
            "a": {
                "b": {
                    "c": TEST_VALUE
                }
            }
        }

        sub = c.sub("subconf")
        del sub["a/b/c"]

        assert "c" not in c.root["subconf"]["a"]["b"]

    def test_subconfig_iter(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c.root["subconf"] = {
            "a": TEST_VALUE,
            "b": TEST_VALUE,
            "c": TEST_VALUE
        }

        assert [x for x in c.sub("subconf")] == ["a", "b", "c"]


class TestConfigTemplate:
    """
    Tests for configuration templates.
    """

    @staticmethod
    def get_template() -> saru.ConfigTemplate:
        """
        Standard template for testing.
        """
        template = saru.ConfigTemplate({
            "a/b/c": TEST_VALUE,
            "a/b/d": TEST_VALUE,
            "a/c/val1": 1,
            "a/c/val2": 2
        })
        return template

    @staticmethod
    def get_expected_structure() -> typing.Mapping[str, saru.ConfigValueT]:
        """
        Expected structure of a config object constructed by
        `TestConfigTemplate.get_template`.
        """
        return {
            "a": {
                "b": {
                    "c": TEST_VALUE,
                    "d": TEST_VALUE
                },
                "c": {
                    "val1": 1,
                    "val2": 2
                }
            }
        }

    @pytest.mark.parametrize("cf", NULL_BACKEND_FACTORIES)
    def test_empty_apply(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        template = self.get_template()
        expected_structure = self.get_expected_structure()

        template.apply(c)
        assert c.root == expected_structure

    @pytest.mark.parametrize("cf", NULL_BACKEND_FACTORIES)
    def test_partially_filled_apply(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        c["a/b/c"] = TEST_VALUE
        c["a/c/val1"] = 1

        template = self.get_template()
        expected_structure = self.get_expected_structure()

        template.apply(c)
        assert c.root == expected_structure

    @pytest.mark.parametrize("cf", NULL_BACKEND_FACTORIES)
    def test_total_filled_apply(self, cf: ConfigFactory) -> None:
        c = cf.new_config()
        template = self.get_template()
        expected_structure = self.get_expected_structure()

        # applying a template twice should not change anything
        template.apply(c)
        template.apply(c)
        assert c.root == expected_structure

    # Can't use null backend factories for this one, since we need a non-null
    # backend.
    def test_rollback_on_error(self) -> None:
        backend = saru.InMemoryConfigBackend()
        c = saru.BaseConfig(backend)

        # populate with some data
        c["config_value"] = TEST_VALUE
        c.write()

        template = saru.ConfigTemplate(
            rollback_on_failure=True,
            paths={
                "possible": TEST_VALUE,  # This will work
                "config_value/impossible": TEST_VALUE  # This won't, causing rollback.
            }
        )

        expected_structure = {
            "config_value": TEST_VALUE
        }

        try:
            template.apply(c)
            c.write()
        except saru.ConfigPathException:
            # Check in-memory, make sure no partial application.
            assert c.root == expected_structure

            # Changes should not have been written either.
            assert backend.data == expected_structure

            return
        except KeyError:
            # This particular test should not throw a KeyError
            assert False, "KeyError thrown!"

        # Should not reach this.
        assert False
