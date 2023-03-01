# twittersphere contributor documentation

Thank you for thinking of contributing to `twittersphere`! We appreciate all feedback
and contributions.

## Ways to contribute

### Filing issues

Found a bug? Have an idea for improvement? Please do not hesitate
to [file an issue on Github](https://github.com/QUT-Digital-Observatory/twittersphere/issues).
If there is already an issue in the list that is similar, you may wish to add your
experience or thoughts as a comment to that issue rather than creating a new issue.

### Contributing to discussions

If you've been using `twittersphere` at all, your feedback and experiences are very
valuable! If you have something to add to the discussion on any of
the [Github issues](https://github.com/QUT-Digital-Observatory/twittersphere/issues), we
very much welcome your thoughts. This may be adding your feedback and ideas to issues
describing new features or improvements to `twittersphere`, or you may be able to help
out with someone else's question or bug report. The more people who are part of the
discussion around `twittersphere`, the better it will be!

### Improving documentation

Notice a typo? Do some instructions need improvement? Is there a useful tip you want to
share? Is a topic wrong, inadequately covered, or simply missing? Even the tiniest docs
contribution makes an enormous improvement to `twittersphere` and for everyone who uses
it.

You can make your changes either through the Github website, or by editing the source
code on your local machine (
see [Setting up to work from the source code](#setting-up-to-work-from-the-source-code)
below).

Either
way, [create a new git branch](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-and-deleting-branches-within-your-repository)
before beginning, so your changes will be kept together.

Please submit documentation improvement work as
a [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
A member of the maintainer team will then review your contributions and respond.

Thank you so much for helping with the docs!

### Fixing bugs and other code contributions

Is there a code change you'd like to make yourself? Thank you! Pull requests are
welcomed. You can find developer documentation for `twittersphere` below.

## Setting up to work from the source code

These instructions assume you have already installed [git](https://git-scm.com/) and
[Python](https://www.python.org/) and have at least a tiny bit of experience with both,
and with working in a command line / terminal environment. If you are new to these
things, welcome! Feel free to contact digitalobservatory@qut.edu.au and we can recommend
some introductory tutorials to suit your own experience and goals.

To set up a development environment:

1. [Clone the repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
   Ensure you are inside the repository root folder in your command line / terminal.
2. \[Optional but recommended] Create a virtual environment for use
   with `twittersphere`. If you use Anaconda or \[mini]conda, use that to create your
   virtual environment. Otherwise,
   use [venv](https://docs.python.org/3/tutorial/venv.html) or your environment manager
   of choice. Ensure you have activated your virtual environment before proceeding.
3. Install `twittersphere` in editable mode so that you're using your local code rather
   than the PyPI version (note the full stop / period at the end of the command):
   ```shell
   pip install -e .
   ```
4. To be able to run the tests, you'll also need to install the tests dependencies:
   ```shell 
   pip install -r requirements-test.txt
   ```
