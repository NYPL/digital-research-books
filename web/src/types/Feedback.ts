export type Feedback = {
  feedback: string;
  category: string;
  url: string;
  email?: string;
};

export type DRBFeedback = Feedback;

export type VRAFeedback = Feedback & {
  sessionId: string;
  thumbState?: string;
};

export type PopupSurveyResponse = {
  responses: string[];
  sessionId: string;
  url: string;
};
