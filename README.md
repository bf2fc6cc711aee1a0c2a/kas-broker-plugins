# Kafka-Group-Authorizer

A authorizer for Strimzi Kafka Operator that can configure ACLs for all users of the cluster with single ACL policy.

The objective is to use this Authorizer with Strimzi Operator when User Operator is not being used to configure a custom Authorizer that can apply to all the users in the kafka cluster. There is provision to configure `superUser` to allow any administrative work.

The Authorizer class and the ACLs for any resource will be defined using Strimzi's Kafka CR. A sample CR fragment looks like

```
kafka:
  spec:
    authorization:
      type: custom
      authorizerClass: io.bf2.kafka.authorizer.CustomAclAuthorizer
      superUsers:
        - CN=sre_user
    config:
      strimzi.authorization.custom-authorizer.allowed-listeners=plain-9092,canary-9096
      strimzi.authorization.custom-authorizer.acl.1: permission=allow;topic=foo;operations=read,write,create
      strimzi.authorization.custom-authorizer.acl.2: permission=allow;topic=*;operations=read
      strimzi.authorization.custom-authorizer.acl.3: permission=deny;group=xyz;operations=read,create
```

"allowed-listeners" defines list of listener names separated by comma(,) which are treated as super user access, any user that made a request using one of this listeners those requests will always be allowed.

Where one could define properties prefixed with “acl.” and incrementing counter to define any number of ACL permissions. Each line would define a single rule for one type of resource. The syntax is a simple key/value pair separated by semicolon(;).

“permission”, resource-type and “operations” are defined as recognized keys

“permission” values are “allow” or “deny” - only one value accepted

resource-type values are “topic”, “group”, “cluster”, “transactional_id” and “delegation_token” which are used as the key. For value, the name of the topic or name of the group or a regular expression matching the name of resource is accepted. For example, a “topic=*” matches all the topics.

“operations” is a comma separated list of operations this rule either allows or denies. The supported values are “all”, “read”, “write”, “create”, “delete”, “alter”, “describe”, “cluster_action”, “describe_configs”, “alter_configs” and “idempotent_write”.

NOTE: All enum properties above match exactly to the enums defined in Kafka Java libraries.

The ACL permissions will be evaluated in the order they are defined, when a rule matches Authorizer will return that rule’s evaluation as a decision. When no rules match, then by default it is denied. So, if ACL is not defined for a resource type then it is automatically gets denied.

## Building

To build the component

```
mvn clean install
```

## Configuring

To configure this with Strimzi, this component needs be built and have the maven artifact available in a Maven repository, that can be reached by the Strimzi build. Then configure the this plugin as a Third Party library such that it will be pulled into the Strimzi Operator image. See `strimzi/docker-images/kafka/kafka-thridparty-libs` and add the dependency to one of the `pom.xml` files and build the Strimzi.

## Releasing

### Release Branch
*Optional - only required when a new release branch is needed -- for patch releases, skip this branch creation, and instead re-use the existing minor release branch*.

If you are starting on main branch, create a new branch from the main. For example `2.5.x`.

```shell
git checkout -b 2.5.x main
git push upstream 2.5.x
```
Now release from the `2.5.x` branch a release of `2.5.0`, if you are already releasing from a branch skip the above
step of creating a new branch and simply checkout that branch.

#### Pull Request
Releases are performed by modifying the `.github/project.yml` file, setting `current-version` to the release version and `next-version` to the next SNAPSHOT. Open a pull request with the changed `project.yml` to initiate the pre-release workflows. The target of the pull request should be either `main` or a release branch (described above).
At this phase, the project milestone will be checked and it will be verified that no issues for the release milestone are still open. Additionally, the project's integration tests will be run.
Once approved and the pull request is merged, the release action will execute. This action will execute the Maven release plugin to tag the release commit, and build the application artifacts. If successful, the action will push the new tag to the GitHub repository and generate release notes listing all of the closed issues included in the milestone. Finally, the milestone will be closed.


