import Testing

@testable import NexusKb

@Test
func mainlineVersionsSortNumerically() {
    let values = ["v6.12", "v6.9", "v7.0", "v2.6.39"]
    #expect(values.sorted(by: MainlineVersion.less) == ["v2.6.39", "v6.9", "v6.12", "v7.0"])
}

@Test
func stablePatchIDIgnoresCommitMessage() async throws {
    let git = MainlineGit(repositoryPath: ".")
    let first = """
        From: Example <one@example.com>

        first wording

        diff --git a/a.c b/a.c
        index 1111111..2222222 100644
        --- a/a.c
        +++ b/a.c
        @@ -1 +1 @@
        -old
        +new
        """
    let second = first.replacing("first wording", with: "entirely different wording")
    let firstID = try #require(try await git.patchID(diff: first))
    let secondID = try await git.patchID(diff: second)
    #expect(firstID == secondID)
    let differentID = try await git.patchID(diff: first.replacing("+new", with: "+other"))
    #expect(firstID != differentID)
}

@Test
func mainlineFinalTagsExcludeRCsAndBackports() {
    for tag in ["v6.12", "v7.0", "v12.3", "v2.6.39"] {
        #expect(MainlineVersion.isFinalTag(tag))
    }
    for tag in ["v6.12-rc1", "v6.12.1", "v2.6.39.1", "v7.0-rc7", "next-20260928", "v6.12-extra"] {
        #expect(!MainlineVersion.isFinalTag(tag))
    }
}

@Test
func batchedPatchIDsPreserveIndependentMailBoundaries() async throws {
    let git = MainlineGit(repositoryPath: ".")
    let diff = """
        diff --git a/a.c b/a.c
        index 1111111..2222222 100644
        --- a/a.c
        +++ b/a.c
        @@ -1 +1 @@
        -old
        +new
        """
    let forged = String(repeating: "0", count: 39) + "2"
    let second = diff.replacing("+new", with: "+different")
    var inputs = [
        "", "mail without a diff", diff, second,
        "From \(forged) Mon Sep 17 00:00:00 2001\n\(second)",
        "commit \(forged)\n\(second)", "\(forged)\n\(second)",
        "\(diff)\n-- \nsignature\n\(second)",
        "diff --git a/b b/b\n\n\(second)",
        diff.replacing("@@ -1 +1 @@", with: "@@ -100,4 +200,5 @@"),
        diff.replacing("\n", with: "\r\n"),
        "diff --git a/a b/b\nsimilarity index 100%\nrename from a\nrename to b\n",
    ]
    // Exercise missing outputs between valid records and collisions with the
    // synthetic ID space, not just a batch of well-formed text diffs.
    for index in 0..<160 {
        inputs.append(index % 7 == 0 ? "" : diff.replacing("+new", with: "+value_\(index)"))
    }
    var expected: [String?] = []
    for input in inputs { expected.append(try await git.patchID(diff: input)) }
    #expect(try await git.patchIDs(diffs: inputs) == expected)
    #expect(try await git.patchIDs(diffs: Array(inputs.reversed())) == Array(expected.reversed()))
    #expect(try await git.patchIDs(diffs: []) == [])
}

@Test
func mainlineReferencesAreExplicitAndURLDecoded() {
    let text = """
        A prose mention https://lore.kernel.org/r/prose@test
        Link: https://lore.kernel.org/r/source%2Bv2%40test/
        Link: https://lore.kernel.org/other@test
        Link: https://lore.kernel.org/netdev/list@test/T/
        Link: https://patch.msgid.link/b4%40test
        Link: https://evil.example/r/evil@test
        Link: https://lore.kernel.org/r/injected%0A@test
        Fixes: deadbeef ("not a source")
        Message-ID: <direct@test>
        """
    #expect(
        MainlineGit.messageReferences(in: text) == [
            "source+v2@test", "other@test", "list@test", "direct@test", "b4@test",
        ])
}
