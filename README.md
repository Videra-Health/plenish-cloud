# Plenish-Cloud

Fork of [Plenish](https://github.com/lambdaisland/plenish) but that works for Datomic Cloud.

The API is identical, but adds the following limitations:
1. Membership attributes in the metaschema.edn may not be cardinality many. If not, deletions will not be synced.
2. Entities must carry the membership attribute on their first assertion. If not, only changes after the membership attribute will by synced.

## License

Copyright &copy; 2023 Arne Brasseur and Contributors
Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
Changes Copyright &copy; 2023 Videra Health