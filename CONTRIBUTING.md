## Contributing

[fork]: https://github.com/lynixliu/SwiftAvroCore

Hi there! We're thrilled that you'd like to contribute to this project. Your help is essential for keeping it great.

Contributions to this project are [released](https://help.github.com/articles/github-terms-of-service/#6-contributions-under-repository-license) to the public under the [project's open source license](LICENSE).

## Branches

* master: The 'master' branch is used for creating stable release builds. This branch is always the same as the latest release. As this project already has an established user base, we kindly request that pull requests are not created directly to this branch. In order to maintain stability and avoid blocking daily builds for our users, this branch should only merge changes from the "develop" branch. Thank you for your cooperation. 
* develop: The "develop" branch is the active development branch. We encourage contributors to create pull requests based on this branch. All pull requests will be merged to "develop" first, and then merged to the "master" branch if all basic tests pass. Once the changes are merged to the "master" branch, a new release tag will be created. Please note that this process helps to ensure stability and maintain a basic level of quality for our users. 

## Submitting a pull request

1. [Fork][fork] and clone the repository
2. Start from develop branch: `git fetch && git checkout develop`
3. Make sure the tests pass on your machine: `swift test`
4. Create a new branch: `git checkout -b my-branch-name`
5. Make your change, add tests, and make sure all tests passed
6. Push to your fork and [submit a pull request to develop branch](https://docs.github.com/en/desktop/contributing-and-collaborating-using-github-desktop/working-with-your-remote-repository-on-github-or-github-enterprise/creating-an-issue-or-pull-request) 
7. Once the code review completed, your pull request would be merged to develop branch.

Here are a few things you can do that will increase the likelihood of your pull request being accepted:

- Write tests.
- Keep your change as focused as possible. If there are multiple changes you would like to make that are not dependent upon each other, consider submitting them as separate pull requests.
- Write a [good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

## Resources

- [How to Contribute to Open Source](https://opensource.guide/how-to-contribute/)
- [Using Pull Requests](https://help.github.com/articles/about-pull-requests/)
- [GitHub Help](https://help.github.com)

