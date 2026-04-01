return { String isoDate ->
    def parsed = Date.parse('yyyy-MM-dd', isoDate)
    parsed.format('MMMM d, yyyy')
}
